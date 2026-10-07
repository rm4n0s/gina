package webpush

// This file is the one place in the package that starts goroutines and uses
// channels, sync and net/http: Gina's TLS is a server and its reactor cannot
// connect out, so requests to push services are made by a small pool of ordinary
// goroutines. They share nothing with isolates except what System.SendExternal
// and the sender's atomic handle allow. Keep it that way.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rm4n0s/gina"
)

// job is a notification accepted by the sender isolate and waiting for a worker.
type job struct {
	n     Notification
	ttl   int64 // seconds
	reply gina.Handle
}

type transport struct {
	w      *WebPush
	jobs   chan *job
	ctx    context.Context
	cancel context.CancelFunc
	client *http.Client
	self   atomic.Uint64 // the sender's live handle

	mu      sync.Mutex
	sys     *gina.System
	started bool
	tokens  map[string]token // VAPID Authorization header per push service origin
	wg      sync.WaitGroup
}

type token struct {
	header  string
	renewAt time.Time
}

func newTransport(w *WebPush) *transport {
	t := &transport{w: w, jobs: make(chan *job, w.cfg.Queue), tokens: map[string]token{}}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.client = w.cfg.Client
	if t.client == nil {
		t.client = defaultClient(w.cfg)
	}
	return t
}

func (t *transport) setSender(h gina.Handle) { t.self.Store(uint64(h)) }
func (t *transport) sender() gina.Handle     { return gina.Handle(t.self.Load()) }

func (t *transport) start(sys *gina.System) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started {
		return errors.New("webpush: already started")
	}
	if t.ctx.Err() != nil {
		return errors.New("webpush: closed")
	}
	t.started, t.sys = true, sys
	for i := 0; i < t.w.cfg.Workers; i++ {
		t.wg.Add(1)
		go t.worker()
	}
	return nil
}

// submit queues j without blocking. False means the queue is full or the workers
// are not running.
func (t *transport) submit(j *job) bool {
	t.mu.Lock()
	ok := t.started && t.ctx.Err() == nil
	t.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case t.jobs <- j:
		return true
	default:
		return false
	}
}

// close cancels in-flight requests and queued work, and waits for the workers.
func (t *transport) close() {
	t.cancel()
	t.wg.Wait()
}

func (t *transport) worker() {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case j := <-t.jobs:
			t.deliver(j)
		}
	}
}

// deliver makes the request, retrying where it is worth it, and reports.
func (t *transport) deliver(j *job) {
	cfg := &t.w.cfg
	res := Result{ID: j.n.ID}
	var body []byte
	if len(j.n.Payload) > 0 {
		var err error
		if body, err = Encrypt(j.n.Sub, j.n.Payload, j.n.Pad); err != nil {
			res.Outcome = OutcomeInvalid
			t.report(j, res)
			return
		}
	} else if err := j.n.Sub.Validate(); err != nil {
		res.Outcome = OutcomeInvalid
		t.report(j, res)
		return
	}
	backoff := cfg.RetryBackoff
	for attempt := 0; ; attempt++ {
		status, retryAfter, err := t.post(j, body)
		if t.ctx.Err() != nil {
			return // shutting down: nobody is listening for a result
		}
		res.Attempts, res.Status = uint8(min(attempt+1, 255)), int32(status)
		switch {
		case err == nil && status >= 200 && status < 300:
			res.Outcome = OutcomeDelivered
		case err == nil && (status == 404 || status == 410):
			res.Outcome = OutcomeGone
		case err == nil && status != 429 && (status < 500 || status > 599):
			res.Outcome = OutcomeRejected
		case attempt < cfg.Retries: // network error, 429 or 5xx: try again
			wait := max(backoff, retryAfter)
			backoff *= 2
			select {
			case <-time.After(min(wait, time.Minute)):
				continue
			case <-t.ctx.Done():
				return
			}
		default:
			res.Outcome = OutcomeFailed
		}
		break
	}
	t.report(j, res)
}

// post makes one request. A non-nil error means no HTTP answer (status 0).
func (t *transport) post(j *job, body []byte) (status int, retryAfter time.Duration, err error) {
	cfg := &t.w.cfg
	auth, err := t.authorization(j.n.Sub.Endpoint)
	if err != nil {
		return 0, 0, err
	}
	ctx, cancel := context.WithTimeout(t.ctx, cfg.Timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.n.Sub.Endpoint, rd)
	if err != nil {
		return 0, 0, err
	}
	h := req.Header
	h.Set("Authorization", auth)
	h.Set("TTL", strconv.FormatInt(j.ttl, 10))
	if body != nil {
		h.Set("Content-Encoding", "aes128gcm")
		h.Set("Content-Type", "application/octet-stream")
	}
	if u := urgencyNames[j.n.Urgency]; u != "" {
		h.Set("Urgency", u)
	}
	if j.n.Topic != "" {
		h.Set("Topic", j.n.Topic)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) // let the connection be reused
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		retryAfter = time.Duration(s) * time.Second
	}
	return resp.StatusCode, retryAfter, nil
}

// authorization returns the VAPID header for the endpoint's origin, signing a
// new token only about twice a day per push service (the token is valid for a
// service, not a subscription).
func (t *transport) authorization(endpoint string) (string, error) {
	aud, err := audience(endpoint)
	if err != nil {
		return "", err
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if tk, ok := t.tokens[aud]; ok && now.Before(tk.renewAt) {
		return tk.header, nil
	}
	h, err := t.w.cfg.VAPID.authorization(endpoint, t.w.cfg.Subject, now.Add(12*time.Hour))
	if err != nil {
		return "", err
	}
	t.tokens[aud] = token{h, now.Add(11 * time.Hour)}
	return h, nil
}

// report sends the result to the isolate that asked, if it still exists.
func (t *transport) report(j *job, r Result) {
	if j.reply == 0 {
		return
	}
	t.mu.Lock()
	sys := t.sys
	t.mu.Unlock()
	sys.SendExternal(j.reply, TagResult, gina.BytesOf(&r))
}

// ---- the default client ----

func defaultClient(cfg Config) *http.Client {
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	if !cfg.AllowPrivate {
		// Control runs after name resolution, on the address about to be dialled, so a
		// hostname that resolves to an internal address (or changes to one between
		// lookups) is stopped too.
		d.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip, err := netip.ParseAddr(host)
			if err != nil || !publicAddr(ip) {
				return errors.New("webpush: refusing to connect to a non-public address")
			}
			return nil
		}
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:         d.DialContext,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConns:        cfg.Workers,
			MaxIdleConnsPerHost: cfg.Workers,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

func publicAddr(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsValid() && !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified() && !cgnat.Contains(ip)
}
