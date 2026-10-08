package webpush

// A delivery is the isolate that sends one notification: it resolves the push
// service's name (asking the resolver isolate), connects, runs the TLS handshake
// (extensions/tls, client side), POSTs the encrypted message over HTTP/1.1 and
// reads the status line and headers of the answer, retrying where that is worth
// it. Then it tells whoever asked how it went and exits.
//
// Everything it waits for is a Gina I/O completion or a message, so it is an
// ordinary state machine on the sender's shard. It owns one socket at a time (the
// connection, one address after another until one answers), which the engine
// closes if the isolate dies. Each notification gets its own connection
// (Connection: close): there is no pool, so a burst to one push service pays one
// TLS handshake per notification.

import (
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

const (
	tagStart   gina.Tag = gina.TagUserBase + 0x34 // sender -> delivery, with the *job attached
	tagRetry   gina.Tag = gina.TagUserBase + 0x35 // timer: the back-off is over
	tagNoReply gina.Tag = gina.TagUserBase + 0x36 // timer: the resolver never answered

	maxHead = 16 << 10 // response headers read before giving up
)

type phase uint8

const (
	phIdle phase = iota
	phResolve
	phConnect
	phSend
	phRecv
	phBackoff
)

// target is the parsed endpoint.
type target struct {
	tls  bool
	host string // without brackets
	ip   netip.Addr
	port uint16
	hdr  string // the Host header
	uri  string
}

func parseTarget(endpoint string) (t target, err error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return t, err
	}
	t.tls = u.Scheme == "https"
	t.host, t.hdr, t.uri = u.Hostname(), u.Host, u.RequestURI()
	port := u.Port()
	if port == "" {
		port = "443"
		if !t.tls {
			port = "80"
		}
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 || t.host == "" {
		return t, errWire
	}
	t.port = uint16(p)
	if ip, err := netip.ParseAddr(t.host); err == nil {
		t.ip = ip.Unmap()
	}
	return t, nil
}

type delivery struct {
	j       *job
	body    []byte // the encrypted message, nil for a notification without data
	tgt     target
	req     []byte
	attempt int
	backoff time.Duration

	phase    phase
	deadline uint64 // Ctx.Now, end of this attempt
	corr     uint32 // identifies the resolver request we are waiting for
	timer    gina.TimerID
	addrs    []netip.Addr
	next     int
	fd       gina.FDHandle

	tc     *gtls.Conn // nil for a plain http endpoint
	pout   []byte     // plain http: bytes waiting to go out
	reqOut bool       // the request has been handed to the connection
	pin    []byte     // plain http: bytes received
	head   []byte     // response bytes so far
	rbuf   []byte
}

func (w *WebPush) deliveryHandler(d *delivery, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagStart:
		j, _ := g.Attachment().(*job)
		if j == nil {
			return gina.Done()
		}
		return d.begin(w, g, j)
	case tagResolved:
		if d.phase != phResolve || m.Correlation != d.corr {
			break // a late answer to an attempt that has moved on
		}
		g.CancelTimer(d.timer)
		return d.resolved(w, g, decodeAddrs(g.Data()))
	case tagNoReply:
		if d.phase == phResolve {
			return d.endAttempt(w, g, 0, 0, false)
		}
	case tagRetry:
		if d.phase == phBackoff {
			return d.attemptStart(w, g)
		}
	case gina.TagIOConnect:
		if d.phase == phConnect {
			return d.connected(w, g, gina.PayloadAs[gina.IOResult](m).Result)
		}
	case gina.TagIOSend:
		if d.phase == phSend {
			return d.sent(w, g, gina.PayloadAs[gina.IOResult](m).Result)
		}
	case gina.TagIORecv:
		if d.phase == phRecv {
			return d.received(w, g, gina.PayloadAs[gina.IOResult](m).Result)
		}
	case gina.TagShutdown:
		return gina.Done() // nobody is listening for a result any more
	}
	return gina.WaitMessage()
}

// begin prepares the message (once: a retry sends the same bytes) and starts.
func (d *delivery) begin(w *WebPush, g *gina.Ctx, j *job) gina.Effect {
	d.j = j
	n := &j.n
	var err error
	if len(n.Payload) > 0 {
		d.body, err = Encrypt(n.Sub, n.Payload, n.Pad)
	} else {
		err = n.Sub.Validate()
	}
	if err == nil {
		d.tgt, err = parseTarget(n.Sub.Endpoint)
	}
	if err != nil {
		return d.report(w, g, Result{ID: n.ID, Outcome: OutcomeInvalid})
	}
	d.req = d.request(w)
	d.backoff = w.cfg.RetryBackoff
	return d.attemptStart(w, g)
}

// request builds the HTTP request (RFC 8030 §5).
func (d *delivery) request(w *WebPush) []byte {
	n := &d.j.n
	var b strings.Builder
	b.WriteString("POST " + d.tgt.uri + " HTTP/1.1\r\nHost: " + d.tgt.hdr + "\r\n")
	b.WriteString("Authorization: " + d.j.auth + "\r\n")
	b.WriteString("TTL: " + strconv.FormatInt(d.j.ttl, 10) + "\r\n")
	if d.body != nil {
		b.WriteString("Content-Encoding: aes128gcm\r\nContent-Type: application/octet-stream\r\n")
	}
	b.WriteString("Content-Length: " + strconv.Itoa(len(d.body)) + "\r\n")
	if u := urgencyNames[n.Urgency]; u != "" {
		b.WriteString("Urgency: " + u + "\r\n")
	}
	if n.Topic != "" {
		b.WriteString("Topic: " + n.Topic + "\r\n")
	}
	b.WriteString("User-Agent: gina-webpush\r\nConnection: close\r\n\r\n")
	return append([]byte(b.String()), d.body...)
}

func (d *delivery) remaining(g *gina.Ctx) time.Duration {
	return time.Duration(int64(d.deadline) - int64(g.Now()))
}

// attemptStart begins one try: find the addresses, then connect.
func (d *delivery) attemptStart(w *WebPush, g *gina.Ctx) gina.Effect {
	d.deadline = g.Now() + uint64(w.cfg.Timeout)
	d.tc, d.pout, d.pin, d.head, d.reqOut = nil, nil, nil, d.head[:0], false
	d.addrs, d.next = nil, 0
	if d.tgt.ip.IsValid() {
		return d.resolved(w, g, []netip.Addr{d.tgt.ip})
	}
	d.phase = phResolve
	d.corr++
	if g.SendCorr(w.resolverHandle(), tagResolve, d.corr, []byte(d.tgt.host)) != gina.SendOK {
		return d.endAttempt(w, g, 0, 0, false)
	}
	// The resolver always answers; the timer is for the day it cannot.
	d.timer = g.RegisterTimer(w.cfg.Timeout, tagNoReply)
	return gina.WaitMessage()
}

func (d *delivery) resolved(w *WebPush, g *gina.Ctx, addrs []netip.Addr) gina.Effect {
	if len(addrs) == 0 {
		return d.endAttempt(w, g, 0, 0, false)
	}
	if !w.cfg.AllowPrivate {
		// A subscriber chooses the endpoint, so the name may lead anywhere. The check
		// is on the addresses we are about to connect to, which no later lookup can
		// change.
		addrs = slices.DeleteFunc(slices.Clone(addrs), func(a netip.Addr) bool { return !publicAddr(a) })
		if len(addrs) == 0 {
			return d.endAttempt(w, g, 0, 0, true) // not worth retrying
		}
	}
	slices.SortStableFunc(addrs, func(a, b netip.Addr) int { // IPv4 first: it works everywhere
		switch {
		case a.Is4() == b.Is4():
			return 0
		case a.Is4():
			return -1
		}
		return 1
	})
	d.addrs, d.next = addrs, 0
	return d.connectNext(w, g)
}

// connectNext dials the next address, if there is one.
func (d *delivery) connectNext(w *WebPush, g *gina.Ctx) gina.Effect {
	for d.next < len(d.addrs) {
		ip := d.addrs[d.next]
		d.next++
		rem := d.remaining(g)
		if rem <= 0 {
			break
		}
		fd, err := g.Dial(gina.DialSpec{IP: ip, Port: d.tgt.port})
		if err != nil {
			continue
		}
		d.fd = fd
		if d.next < len(d.addrs) { // leave time for the others
			rem = min(rem, 5*time.Second)
		}
		d.phase = phConnect
		g.IOConnect(fd, rem)
		return gina.WaitIO()
	}
	return d.endAttempt(w, g, 0, 0, false)
}

func (d *delivery) connected(w *WebPush, g *gina.Ctx, res int64) gina.Effect {
	if res < 0 {
		g.CloseFD(d.fd)
		d.fd = 0
		return d.connectNext(w, g)
	}
	if d.tgt.tls {
		tc, err := gtls.NewClient(&gtls.ClientConfig{ServerName: d.tgt.host, RootCAs: w.cfg.RootCAs, NextProtos: []string{"http/1.1"}})
		if err != nil {
			return d.endAttempt(w, g, 0, 0, true)
		}
		d.tc = tc
	} else {
		d.pout, d.reqOut = d.req, true
	}
	d.rbuf = make([]byte, 8<<10)
	return d.pump(w, g)
}

// pump does whatever the connection needs next: send what is queued, else read.
func (d *delivery) pump(w *WebPush, g *gina.Ctx) gina.Effect {
	for {
		out := d.pout
		if d.tc != nil {
			out = d.tc.Outgoing()
		}
		if len(out) > 0 {
			rem := d.remaining(g)
			if rem <= 0 {
				return d.endAttempt(w, g, 0, 0, false)
			}
			d.phase = phSend
			g.IOSend(d.fd, out, rem)
			return gina.WaitIO()
		}
		if d.tc == nil {
			break
		}
		if d.tc.Err() != nil {
			return d.endAttempt(w, g, 0, 0, false)
		}
		if !d.tc.HandshakeComplete() {
			break
		}
		if !d.reqOut {
			d.reqOut = true
			if d.tc.Write(d.req) != nil {
				return d.endAttempt(w, g, 0, 0, false)
			}
			continue
		}
		if n := d.tc.PlainLen(); n > 0 {
			buf := make([]byte, n)
			d.tc.ReadPlain(buf)
			d.head = append(d.head, buf...)
		}
		break
	}
	if d.tc == nil && len(d.pin) > 0 {
		d.head, d.pin = append(d.head, d.pin...), nil
	}
	if status, retryAfter, ok, bad := parseHead(&d.head); bad {
		return d.endAttempt(w, g, 0, 0, false)
	} else if ok {
		return d.endAttempt(w, g, status, retryAfter, false)
	}
	rem := d.remaining(g)
	if rem <= 0 {
		return d.endAttempt(w, g, 0, 0, false)
	}
	d.phase = phRecv
	g.IORecv(d.fd, d.rbuf, rem)
	return gina.WaitIO()
}

func (d *delivery) sent(w *WebPush, g *gina.Ctx, res int64) gina.Effect {
	if res < 0 {
		return d.endAttempt(w, g, 0, 0, false)
	}
	if d.tc != nil {
		d.tc.ConsumeOut(int(res))
	} else {
		d.pout = nil
	}
	return d.pump(w, g)
}

func (d *delivery) received(w *WebPush, g *gina.Ctx, res int64) gina.Effect {
	if res <= 0 { // timed out, reset, or closed before it answered
		return d.endAttempt(w, g, 0, 0, false)
	}
	data := d.rbuf[:res]
	if d.tc != nil {
		if d.tc.Feed(data) != nil { // an alert may be queued; there is no point sending it
			return d.endAttempt(w, g, 0, 0, false)
		}
	} else {
		d.pin = append(d.pin, data...)
	}
	return d.pump(w, g)
}

// parseHead looks for a complete final response head in *buf. ok means status and
// retryAfter are valid; bad means the peer is not speaking HTTP (or the head is
// absurdly long). 1xx responses are skipped.
func parseHead(buf *[]byte) (status int, retryAfter time.Duration, ok, bad bool) {
	for {
		b := *buf
		end := strings.Index(string(b), "\r\n\r\n")
		if end < 0 {
			return 0, 0, false, len(b) > maxHead
		}
		lines := strings.Split(string(b[:end]), "\r\n")
		f := strings.Fields(lines[0])
		if len(f) < 2 || !strings.HasPrefix(f[0], "HTTP/1.") {
			return 0, 0, false, true
		}
		s, err := strconv.Atoi(f[1])
		if err != nil || s < 100 || s > 599 {
			return 0, 0, false, true
		}
		if s < 200 {
			*buf = b[end+4:]
			continue
		}
		for _, l := range lines[1:] {
			if k, v, found := strings.Cut(l, ":"); found && strings.EqualFold(k, "Retry-After") {
				if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
					retryAfter = time.Duration(secs) * time.Second
				}
			}
		}
		return s, retryAfter, true, false
	}
}

// endAttempt closes the connection and decides what the outcome of the try means:
// done, or worth another go. status 0 means there was no HTTP answer. permanent
// marks a failure that retrying cannot fix.
func (d *delivery) endAttempt(w *WebPush, g *gina.Ctx, status int, retryAfter time.Duration, permanent bool) gina.Effect {
	if d.fd != 0 {
		g.CloseFD(d.fd)
		d.fd = 0
	}
	d.tc, d.phase = nil, phIdle
	cfg := &w.cfg
	res := Result{ID: d.j.n.ID, Attempts: uint8(min(d.attempt+1, 255)), Status: int32(status)}
	switch {
	case status >= 200 && status < 300:
		res.Outcome = OutcomeDelivered
	case status == 404 || status == 410:
		res.Outcome = OutcomeGone
	case status != 0 && status != 429 && (status < 500 || status > 599):
		res.Outcome = OutcomeRejected
	case !permanent && d.attempt < cfg.Retries: // network error, 429 or 5xx: try again
		wait := min(max(d.backoff, retryAfter), time.Minute)
		d.backoff *= 2
		d.attempt++
		d.phase = phBackoff
		if g.RegisterTimer(wait, tagRetry) != 0 {
			return gina.WaitMessage()
		}
		res.Outcome = OutcomeFailed
	default:
		res.Outcome = OutcomeFailed
	}
	return d.report(w, g, res)
}

// report sends the result to the isolate that asked, if it still exists.
func (d *delivery) report(w *WebPush, g *gina.Ctx, r Result) gina.Effect {
	w.reply(g, d.j.reply, r)
	d.j.reported = true
	return gina.Done()
}

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// publicAddr reports whether ip is somewhere a push service could be: not
// loopback, private, link-local, multicast, unspecified or carrier-grade NAT.
func publicAddr(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsValid() && !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified() && !cgnat.Contains(ip)
}
