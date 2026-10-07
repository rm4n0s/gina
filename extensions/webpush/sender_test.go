package webpush

import (
	"crypto/ecdh"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

// ---- a fake push service: it behaves like a browser's vendor, decrypting what it gets ----

type seen struct {
	path    string
	header  http.Header
	body    []byte
	payload []byte // decrypted
	derr    error
}

type pushSvc struct {
	*httptest.Server
	ua   *ecdh.PrivateKey
	auth []byte
	mu   sync.Mutex
	got  []seen
	hits map[string]int
	hold chan struct{} // /hold blocks until closed
}

func newPushSvc(t *testing.T) *pushSvc {
	ua, sub := newUA(t)
	p := &pushSvc{ua: ua, auth: sub.Auth, hits: map[string]int{}, hold: make(chan struct{})}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s := seen{path: r.URL.Path, header: r.Header.Clone(), body: body}
		if len(body) > 0 {
			s.payload, s.derr = decrypt(ua, sub.Auth, body)
		}
		p.mu.Lock()
		p.got = append(p.got, s)
		p.hits[r.URL.Path]++
		n := p.hits[r.URL.Path]
		p.mu.Unlock()
		switch r.URL.Path {
		case "/gone":
			w.WriteHeader(410)
		case "/missing":
			w.WriteHeader(404)
		case "/forbidden":
			w.WriteHeader(403)
		case "/down":
			w.WriteHeader(503)
		case "/flaky": // fails twice, then works
			if n <= 2 {
				w.WriteHeader(503)
				return
			}
			w.WriteHeader(201)
		case "/throttled":
			if n == 1 {
				w.WriteHeader(429)
				return
			}
			w.WriteHeader(201)
		case "/hold":
			<-p.hold
			w.WriteHeader(201)
		default:
			w.WriteHeader(201)
		}
	}))
	t.Cleanup(p.Server.Close)
	t.Cleanup(func() {
		select {
		case <-p.hold:
		default:
			close(p.hold)
		}
	})
	return p
}

func (p *pushSvc) sub(path string) Subscription {
	return Subscription{Endpoint: p.URL + path, P256dh: p.ua.PublicKey().Bytes(), Auth: p.auth}
}

func (p *pushSvc) requests(path string) []seen {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []seen
	for _, s := range p.got {
		if s.path == path {
			out = append(out, s)
		}
	}
	return out
}

// ---- a system with a collector isolate on shard 0 and the sender on shard 1 ----

const (
	typeCollector gina.TypeID = 1
	tagDo         gina.Tag    = gina.TagUserBase + 1
)

type collector struct{}

type harness struct {
	t       *testing.T
	sys     *gina.System
	wp      *WebPush
	vapid   *VAPID
	results chan Result
	todo    chan *Notification
	coll    gina.Handle
}

func newHarness(t *testing.T, cfg Config, start bool) *harness {
	t.Helper()
	h := &harness{t: t, results: make(chan Result, 64), todo: make(chan *Notification, 64), coll: gina.MakeHandle(0, typeCollector, 0, 1)}
	var err error
	if cfg.VAPID == nil {
		if h.vapid, err = GenerateVAPID(); err != nil {
			t.Fatal(err)
		}
		cfg.VAPID = h.vapid
	} else {
		h.vapid = cfg.VAPID
	}
	if cfg.Subject == "" {
		cfg.Subject = "mailto:test@example.com"
	}
	cfg.Shard = 1
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = time.Millisecond
	}
	if h.wp, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{gina.RegisterType(typeCollector, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 256}, nil,
			func(_ *collector, ctx *gina.Ctx, m *gina.Message) gina.Effect {
				switch m.Tag {
				case tagDo: // send what the test queued, as this isolate
					select {
					case n := <-h.todo:
						if r := h.wp.Send(ctx, n); r != gina.SendOK {
							t.Errorf("Send: %v", r)
						}
					default:
					}
				case TagResult:
					h.results <- *gina.PayloadAs[Result](m)
				case gina.TagShutdown:
					return gina.Done()
				}
				return gina.WaitMessage()
			})},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: typeCollector, Group: gina.GroupRoot, Restart: gina.RestartPermanent}}}, {}},
	}
	if err := h.wp.Install(&spec); err != nil {
		t.Fatal(err)
	}
	if h.sys, err = gina.NewSystem(spec, gina.Options{}); err != nil {
		t.Fatal(err)
	}
	if start {
		if err := h.wp.Start(h.sys); err != nil {
			t.Fatal(err)
		}
	}
	h.sys.Start(gina.RunOptions{})
	t.Cleanup(func() { h.sys.Stop(); h.sys.Wait(); h.wp.Close(); h.sys.Close() })
	return h
}

// send queues n and has the collector isolate send it.
func (h *harness) send(n *Notification) {
	h.t.Helper()
	h.todo <- n
	if r := h.sys.SendExternal(h.coll, tagDo, nil); r != gina.SendOK {
		h.t.Fatalf("poke: %v", r)
	}
}

func (h *harness) result() Result {
	h.t.Helper()
	select {
	case r := <-h.results:
		return r
	case <-time.After(5 * time.Second):
		h.t.Fatal("no result")
		return Result{}
	}
}

func (h *harness) noResult() {
	h.t.Helper()
	select {
	case r := <-h.results:
		h.t.Fatalf("unexpected result %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
}

func dev() Config { return Config{AllowInsecure: true, AllowPrivate: true} }

// ---- tests ----

func TestSendDelivers(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), true)
	h.send(&Notification{ID: 42, Sub: svc.sub("/ok"), Payload: []byte(`{"title":"hi"}`), TTL: 90 * time.Second, Urgency: UrgencyHigh, Topic: "news_1"})
	r := h.result() // comes back to the collector because it called Send: m.Source
	if r.ID != 42 || r.Outcome != OutcomeDelivered || r.Status != 201 || r.Attempts != 1 {
		t.Fatalf("result %+v", r)
	}
	reqs := svc.requests("/ok")
	if len(reqs) != 1 {
		t.Fatalf("%d requests", len(reqs))
	}
	q := reqs[0]
	if q.derr != nil || string(q.payload) != `{"title":"hi"}` {
		t.Fatalf("payload %q, %v", q.payload, q.derr)
	}
	hd := q.header
	if hd.Get("Content-Encoding") != "aes128gcm" || hd.Get("TTL") != "90" || hd.Get("Urgency") != "high" || hd.Get("Topic") != "news_1" {
		t.Fatalf("headers %v", hd)
	}
	a := hd.Get("Authorization")
	jwt, k, ok := strings.Cut(strings.TrimPrefix(a, "vapid t="), ", k=")
	if !ok || k != h.vapid.PublicKey() {
		t.Fatalf("Authorization %q", a)
	}
	c := verifyJWT(t, jwt, mustB64(t, k))
	if c.Aud != svc.URL || c.Sub != "mailto:test@example.com" || time.Until(time.Unix(c.Exp, 0)) > 24*time.Hour || time.Until(time.Unix(c.Exp, 0)) < time.Hour {
		t.Fatalf("claims %+v (want aud %s)", c, svc.URL)
	}
}

func TestSendWithoutPayloadSendsNoBody(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), true)
	h.send(&Notification{ID: 1, Sub: svc.sub("/ok")})
	if r := h.result(); r.Outcome != OutcomeDelivered {
		t.Fatalf("%+v", r)
	}
	q := svc.requests("/ok")[0]
	if len(q.body) != 0 || q.header.Get("Content-Encoding") != "" || q.header.Get("TTL") != "86400" || q.header.Get("Urgency") != "" {
		t.Fatalf("body %d bytes, headers %v", len(q.body), q.header)
	}
}

func TestOutcomes(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, func() Config { c := dev(); c.Retries = 2; return c }(), true)
	for _, tc := range []struct {
		path     string
		outcome  Outcome
		status   int32
		attempts uint8
	}{
		{"/gone", OutcomeGone, 410, 1},
		{"/missing", OutcomeGone, 404, 1},
		{"/forbidden", OutcomeRejected, 403, 1}, // not retried
		{"/down", OutcomeFailed, 503, 3},        // 1 + 2 retries
		{"/flaky", OutcomeDelivered, 201, 3},
		{"/throttled", OutcomeDelivered, 201, 2},
	} {
		h.send(&Notification{ID: 7, Sub: svc.sub(tc.path), Payload: []byte("x")})
		r := h.result()
		if r.Outcome != tc.outcome || r.Status != tc.status || r.Attempts != tc.attempts {
			t.Errorf("%s: got %v status %d attempts %d, want %v %d %d", tc.path, r.Outcome, r.Status, r.Attempts, tc.outcome, tc.status, tc.attempts)
		}
		if n := len(svc.requests(tc.path)); n != int(tc.attempts) {
			t.Errorf("%s: server saw %d requests, want %d", tc.path, n, tc.attempts)
		}
	}
	// A retry resends the same encrypted body (the receiver can decrypt each).
	for _, q := range svc.requests("/flaky") {
		if q.derr != nil || string(q.payload) != "x" {
			t.Errorf("retry body: %q %v", q.payload, q.derr)
		}
	}
}

func TestRetriesCanBeDisabled(t *testing.T) {
	svc := newPushSvc(t)
	c := dev()
	c.Retries = -1
	h := newHarness(t, c, true)
	h.send(&Notification{ID: 1, Sub: svc.sub("/down")})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Attempts != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestInvalidNotificationsSendNothing(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), true)
	good := svc.sub("/ok")
	badKey := good
	badKey.P256dh = append([]byte{4}, make([]byte, 64)...)
	for name, n := range map[string]*Notification{
		"too large":     {Sub: good, Payload: make([]byte, MaxPayload+1)},
		"bad topic":     {Sub: good, Topic: "has space"},
		"long topic":    {Sub: good, Topic: strings.Repeat("a", 33)},
		"bad urgency":   {Sub: good, Urgency: 9},
		"pad too large": {Sub: good, Payload: make([]byte, 10), Pad: MaxPayload},
		"off-curve key": {Sub: badKey, Payload: []byte("x")},
		"no endpoint":   {Sub: Subscription{P256dh: good.P256dh, Auth: good.Auth}},
		"userinfo":      {Sub: Subscription{Endpoint: "http://u:p@" + strings.TrimPrefix(svc.URL, "http://") + "/ok", P256dh: good.P256dh, Auth: good.Auth}},
	} {
		n.ID = 5
		h.send(n)
		if r := h.result(); r.Outcome != OutcomeInvalid || r.ID != 5 || r.Attempts != 0 {
			t.Errorf("%s: %+v", name, r)
		}
	}
	if len(svc.got) != 0 {
		t.Fatalf("%d requests reached the service", len(svc.got))
	}
}

func TestHTTPEndpointNeedsAllowInsecure(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, Config{AllowPrivate: true}, true) // AllowInsecure off
	h.send(&Notification{ID: 1, Sub: svc.sub("/ok")})
	if r := h.result(); r.Outcome != OutcomeInvalid {
		t.Fatalf("%+v", r)
	}
	if len(svc.got) != 0 {
		t.Fatal("request was made")
	}
}

// A subscriber chooses the endpoint, so by default the sender must not connect to
// loopback or private addresses.
func TestDefaultClientRefusesPrivateAddresses(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, Config{AllowInsecure: true, Retries: -1}, true)
	h.send(&Notification{ID: 1, Sub: svc.sub("/ok")})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Status != 0 {
		t.Fatalf("%+v", r)
	}
	if len(svc.got) != 0 {
		t.Fatal("the loopback service was reached")
	}
}

func TestPublicAddr(t *testing.T) {
	for ip, want := range map[string]bool{
		"8.8.8.8": true, "142.250.1.1": true, "2606:4700::1111": true,
		"127.0.0.1": false, "::1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false,
		"169.254.169.254": false, "100.64.0.1": false, "0.0.0.0": false, "::": false, "fe80::1": false, "fd00::1": false,
		"224.0.0.1": false, "::ffff:127.0.0.1": false, "::ffff:10.0.0.1": false, "::ffff:8.8.8.8": true,
	} {
		a, err := parseAddr(ip)
		if err != nil {
			t.Fatal(ip, err)
		}
		if got := publicAddr(a); got != want {
			t.Errorf("publicAddr(%s) = %v, want %v", ip, got, want)
		}
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	var hit atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Add(1) }))
	defer target.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redir.Close()
	_, sub := newUA(t)
	sub.Endpoint = redir.URL + "/x"
	h := newHarness(t, dev(), true)
	h.send(&Notification{ID: 1, Sub: sub})
	if r := h.result(); r.Outcome != OutcomeRejected || r.Status != 307 {
		t.Fatalf("%+v", r)
	}
	if hit.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}

func TestVAPIDTokenIsReusedPerOrigin(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), true)
	for i := 0; i < 3; i++ {
		h.send(&Notification{ID: uint64(i), Sub: svc.sub("/ok")})
		h.result()
	}
	reqs := svc.requests("/ok")
	if len(reqs) != 3 || reqs[0].header.Get("Authorization") != reqs[1].header.Get("Authorization") || reqs[1].header.Get("Authorization") != reqs[2].header.Get("Authorization") {
		t.Fatal("token was signed again for every push")
	}
}

func TestReplyToOverridesSource(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), true)
	// Sent from outside the system there is no source: the result goes to ReplyTo.
	if r := h.wp.SendExternal(h.sys, &Notification{ID: 9, Sub: svc.sub("/ok"), ReplyTo: h.coll}); r != gina.SendOK {
		t.Fatal(r)
	}
	if r := h.result(); r.ID != 9 || r.Outcome != OutcomeDelivered {
		t.Fatalf("%+v", r)
	}
	// And with neither, the notification is still sent and nobody hears of it.
	h.wp.SendExternal(h.sys, &Notification{ID: 10, Sub: svc.sub("/ok")})
	h.noResult()
	if n := len(svc.requests("/ok")); n != 2 {
		t.Fatalf("%d requests, want 2", n)
	}
}

func TestOverloadedWhenQueueFull(t *testing.T) {
	svc := newPushSvc(t)
	c := dev()
	c.Workers, c.Queue = 1, 1
	h := newHarness(t, c, true)
	// One is in flight at the service, one is queued, the rest have nowhere to go.
	for i := 0; i < 6; i++ {
		h.send(&Notification{ID: uint64(i), Sub: svc.sub("/hold")})
	}
	over := 0
	for i := 0; i < 4; i++ {
		if r := h.result(); r.Outcome == OutcomeOverloaded {
			over++
		} else {
			t.Fatalf("unexpected %+v", r)
		}
	}
	if over != 4 {
		t.Fatalf("%d overloaded", over)
	}
	close(svc.hold)
	for i := 0; i < 2; i++ {
		if r := h.result(); r.Outcome != OutcomeDelivered {
			t.Fatalf("%+v", r)
		}
	}
}

func TestNotStartedAnswersOverloaded(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), false)
	h.send(&Notification{ID: 1, Sub: svc.sub("/ok")})
	if r := h.result(); r.Outcome != OutcomeOverloaded {
		t.Fatalf("%+v", r)
	}
	if err := h.wp.Start(h.sys); err != nil {
		t.Fatal(err)
	}
	if err := h.wp.Start(h.sys); err == nil {
		t.Fatal("second Start accepted")
	}
	h.send(&Notification{ID: 2, Sub: svc.sub("/ok")})
	if r := h.result(); r.Outcome != OutcomeDelivered {
		t.Fatalf("%+v", r)
	}
}

func TestShutdownAbandonsInFlightWork(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev(), true)
	h.send(&Notification{ID: 1, Sub: svc.sub("/hold")})
	for i := 0; ; i++ { // wait until the request is at the service
		svc.mu.Lock()
		n := svc.hits["/hold"]
		svc.mu.Unlock()
		if n == 1 {
			break
		}
		if i > 500 {
			t.Fatal("request never arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { h.sys.Stop(); h.sys.Wait(); h.wp.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown waited for the push service")
	}
	h.wp.Close() // idempotent
	if err := h.wp.Start(h.sys); err == nil {
		t.Fatal("Start after Close accepted")
	}
}

func TestSenderHandleIsLive(t *testing.T) {
	h := newHarness(t, dev(), true)
	s := h.wp.Sender()
	if s.Shard() != 1 || s.Type() != 220 {
		t.Fatalf("sender %v", s)
	}
}

func TestWireRoundTrip(t *testing.T) {
	_, sub := newUA(t)
	for _, n := range []Notification{
		{ID: 1, Sub: sub},
		{ID: 1<<64 - 1, Sub: sub, Payload: []byte("hello"), TTL: 3 * time.Hour, Urgency: UrgencyVeryLow, Topic: "t-1", Pad: 12, ReplyTo: 0xdead},
		{ID: 3, Sub: sub, Payload: make([]byte, MaxPayload), TTL: -1},
	} {
		got, ttl, err := unmarshal(n.marshal())
		if err != nil {
			t.Fatal(err)
		}
		want := int64(-1)
		if n.TTL < 0 {
			want = 0
		} else if n.TTL > 0 {
			want = int64(n.TTL / time.Second)
		}
		if ttl != want || got.ID != n.ID || got.ReplyTo != n.ReplyTo || got.Urgency != n.Urgency || got.Topic != n.Topic || got.Pad != n.Pad ||
			got.Sub.Endpoint != sub.Endpoint || string(got.Sub.P256dh) != string(sub.P256dh) || string(got.Sub.Auth) != string(sub.Auth) || string(got.Payload) != string(n.Payload) {
			t.Fatalf("round trip changed %+v into %+v (ttl %d)", n, got, ttl)
		}
	}
}

func TestWireRejectsGarbage(t *testing.T) {
	_, sub := newUA(t)
	good := (&Notification{ID: 1, Sub: sub, Topic: "x"}).marshal()
	for i := 0; i < len(good); i++ { // every truncation
		if _, _, err := unmarshal(good[:i]); err == nil { // the topic is the last field and the payload empty, so any cut is an error
			t.Fatalf("accepted %d-byte prefix", i)
		}
	}
	rnd := make([]byte, 200)
	for i := 0; i < 2000; i++ { // must not panic
		rand.Read(rnd)
		unmarshal(rnd[:i%len(rnd)])
		rnd[0] = wireVersion
		unmarshal(rnd[:i%len(rnd)])
	}
	if _, _, err := unmarshal(append([]byte{2}, good[1:]...)); err == nil {
		t.Fatal("wrong version accepted")
	}
}

func TestResultFitsAMessage(t *testing.T) {
	if err := gina.ValidatePayloadType[Result](); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidation(t *testing.T) {
	v, _ := GenerateVAPID()
	if _, err := New(Config{Subject: "mailto:a@b.c"}); err == nil {
		t.Error("missing VAPID accepted")
	}
	if _, err := New(Config{VAPID: v}); err == nil {
		t.Error("missing Subject accepted")
	}
	w, _ := New(Config{VAPID: v, Subject: "mailto:a@b.c", Shard: 3})
	if err := w.Install(&gina.SystemSpec{Shards: make([]gina.ShardSpec, 2)}); err == nil {
		t.Error("Shard outside the spec accepted")
	}
}

func parseAddr(s string) (netip.Addr, error) { return netip.ParseAddr(s) }
