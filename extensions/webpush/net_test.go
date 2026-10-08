package webpush

import (
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- the DNS codec ----

func TestDNSQueryRoundTrip(t *testing.T) {
	q, err := dnsQuery(0xBEEF, "Push.Example.com.", dnsAAAA)
	if err != nil {
		t.Fatal(err)
	}
	// Turn the query into an answer: QR set, one AAAA record, the OPT record dropped.
	end := 12 + len("\x04push\x07example\x03com\x00") + 4
	a := append([]byte(nil), q[:end]...)
	binary.BigEndian.PutUint16(a[2:], 0x8180)
	binary.BigEndian.PutUint16(a[6:], 2)
	binary.BigEndian.PutUint16(a[10:], 0)
	a = append(a, 0xC0, 12, 0, dnsAAAA, 0, 1, 0, 0, 0, 60, 0, 16)
	a = append(a, netip.MustParseAddr("2001:db8::1").AsSlice()...)
	a = append(a, 0xC0, 12, 0, dnsAAAA, 0, 1, 0, 0, 0, 30, 0, 16)
	a = append(a, netip.MustParseAddr("2001:db8::2").AsSlice()...)
	got, err := dnsParse(a, 0xBEEF, "push.example.com", dnsAAAA)
	if err != nil || got.rcode != 0 || len(got.addrs) != 2 || got.addrs[1] != netip.MustParseAddr("2001:db8::2") || got.ttl != 30 {
		t.Fatalf("%+v %v", got, err)
	}
	for name, mutate := range map[string]func([]byte) ([]byte, uint16, string, uint16){
		"wrong id":   func(b []byte) ([]byte, uint16, string, uint16) { return b, 1, "push.example.com", dnsAAAA },
		"wrong name": func(b []byte) ([]byte, uint16, string, uint16) { return b, 0xBEEF, "evil.example.com", dnsAAAA },
		"wrong type": func(b []byte) ([]byte, uint16, string, uint16) { return b, 0xBEEF, "push.example.com", dnsA },
		"not a response": func(b []byte) ([]byte, uint16, string, uint16) {
			b = append([]byte(nil), b...)
			b[2] = 0
			return b, 0xBEEF, "push.example.com", dnsAAAA
		},
		"truncated": func(b []byte) ([]byte, uint16, string, uint16) {
			return b[:len(b)-5], 0xBEEF, "push.example.com", dnsAAAA
		},
		"short": func(b []byte) ([]byte, uint16, string, uint16) { return b[:7], 0xBEEF, "push.example.com", dnsAAAA },
	} {
		b, id, n, qt := mutate(a)
		if _, err := dnsParse(b, id, n, qt); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for i := 0; i < len(a); i++ { // no prefix may panic
		dnsParse(a[:i], 0xBEEF, "push.example.com", dnsAAAA)
	}
	for _, bad := range []string{"", ".", "a..b", strings.Repeat("a", 64) + ".com", strings.Repeat("a.", 130) + "com", "münchen.de", "sp ace.com"} {
		if _, err := dnsQuery(1, bad, dnsA); err == nil {
			t.Errorf("query for %q accepted", bad)
		}
	}
}

func TestAddrEncoding(t *testing.T) {
	in := []netip.Addr{netip.MustParseAddr("1.2.3.4"), netip.MustParseAddr("2001:db8::1")}
	got := decodeAddrs(encodeAddrs(nil, in))
	if len(got) != 2 || got[0] != in[0] || got[1] != in[1] {
		t.Fatalf("%v", got)
	}
	if len(decodeAddrs([]byte{4, 1, 2})) != 0 || len(decodeAddrs([]byte{7, 1})) != 0 {
		t.Fatal("garbage decoded")
	}
}

// ---- a fake DNS server ----

type fakeDNS struct {
	pc      net.PacketConn
	mu      sync.Mutex
	queries map[string]int // "name/type" -> count
	answer  func(name string, qtype uint16) (rcode int, addrs []netip.Addr)
	silent  atomic.Bool // read and ignore everything
}

func newFakeDNS(t *testing.T, answer func(string, uint16) (int, []netip.Addr)) *fakeDNS {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &fakeDNS{pc: pc, queries: map[string]int{}, answer: answer}
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if d.silent.Load() {
				continue
			}
			q := buf[:n]
			// the name is at 12.. up to the zero label
			end := 12
			var labels []string
			for q[end] != 0 {
				labels = append(labels, string(q[end+1:end+1+int(q[end])]))
				end += 1 + int(q[end])
			}
			qtype := binary.BigEndian.Uint16(q[end+1:])
			name := strings.ToLower(strings.Join(labels, "."))
			d.mu.Lock()
			d.queries[fmt.Sprintf("%s/%d", name, qtype)]++
			d.mu.Unlock()
			rcode, addrs := d.answer(name, qtype)
			r := append([]byte(nil), q[:end+5]...)
			binary.BigEndian.PutUint16(r[2:], 0x8180|uint16(rcode))
			binary.BigEndian.PutUint16(r[10:], 0)
			var rrs []byte
			count := 0
			// a CNAME in front, as real answers have
			if rcode == 0 && len(addrs) > 0 {
				rrs = append(rrs, 0xC0, 12, 0, 5, 0, 1, 0, 0, 0, 60, 0, 2, 0xC0, 12)
				count++
			}
			for _, a := range addrs {
				if (qtype == dnsA) != a.Is4() {
					continue
				}
				rrs = append(rrs, 0xC0, 12, byte(qtype>>8), byte(qtype), 0, 1, 0, 0, 0, 60, 0, byte(a.BitLen()/8))
				rrs = append(rrs, a.AsSlice()...)
				count++
			}
			binary.BigEndian.PutUint16(r[6:], uint16(count))
			pc.WriteTo(append(r, rrs...), from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return d
}

func (d *fakeDNS) addr() netip.AddrPort { return netip.MustParseAddrPort(d.pc.LocalAddr().String()) }

func (d *fakeDNS) count(name string) (a, aaaa int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.queries[fmt.Sprintf("%s/%d", name, dnsA)], d.queries[fmt.Sprintf("%s/%d", name, dnsAAAA)]
}

var loopback = []netip.Addr{netip.MustParseAddr("127.0.0.1")}

func answerWith(addrs ...netip.Addr) func(string, uint16) (int, []netip.Addr) {
	return func(string, uint16) (int, []netip.Addr) { return 0, addrs }
}

func noHosts() map[string][]netip.Addr { return map[string][]netip.Addr{} }

// portOf returns the port of an httptest server.
func portOf(t *testing.T, svc *pushSvc) string {
	_, p, err := net.SplitHostPort(strings.TrimPrefix(strings.TrimPrefix(svc.URL, "https://"), "http://"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func named(t *testing.T, svc *pushSvc, scheme, host, path string) Subscription {
	s := svc.sub(path)
	s.Endpoint = scheme + "://" + host + ":" + portOf(t, svc) + path
	return s
}

// ---- name resolution through the resolver and lookup isolates ----

func TestResolvesNamesWithDNS(t *testing.T) {
	svc := newPushSvc(t)
	dns := newFakeDNS(t, answerWith(loopback[0]))
	c := dev()
	c.Resolvers, c.Hosts = []netip.AddrPort{dns.addr()}, noHosts()
	h := newHarness(t, c)
	h.send(&Notification{ID: 1, Sub: named(t, svc, "http", "push.test", "/ok"), Payload: []byte("hi")})
	if r := h.result(); r.Outcome != OutcomeDelivered || r.Attempts != 1 {
		t.Fatalf("%+v", r)
	}
	if q := svc.requests("/ok"); len(q) != 1 || q[0].header.Get("Host") != "" && !strings.HasPrefix(q[0].header.Get("Host"), "push.test:") {
		t.Fatalf("requests %+v", q)
	}
	if a, aaaa := dns.count("push.test"); a != 1 || aaaa != 1 {
		t.Fatalf("queries A=%d AAAA=%d, want one of each", a, aaaa)
	}

	// The answer is cached: more pushes ask nothing, and a burst is served at once.
	for i := 0; i < 5; i++ {
		h.send(&Notification{ID: uint64(10 + i), Sub: named(t, svc, "http", "push.test", "/ok")})
	}
	for i := 0; i < 5; i++ {
		if r := h.result(); r.Outcome != OutcomeDelivered {
			t.Fatalf("%+v", r)
		}
	}
	if a, aaaa := dns.count("push.test"); a != 1 || aaaa != 1 {
		t.Fatalf("cache missed: A=%d AAAA=%d", a, aaaa)
	}
}

func TestConcurrentLookupsOfOneNameAreMerged(t *testing.T) {
	svc := newPushSvc(t)
	dns := newFakeDNS(t, func(string, uint16) (int, []netip.Addr) {
		time.Sleep(50 * time.Millisecond) // slow enough that every send arrives while it is pending
		return 0, loopback
	})
	c := dev()
	c.Resolvers, c.Hosts = []netip.AddrPort{dns.addr()}, noHosts()
	h := newHarness(t, c)
	for i := 0; i < 6; i++ {
		h.send(&Notification{ID: uint64(i), Sub: named(t, svc, "http", "merge.test", "/ok")})
	}
	for i := 0; i < 6; i++ {
		if r := h.result(); r.Outcome != OutcomeDelivered {
			t.Fatalf("%+v", r)
		}
	}
	if a, aaaa := dns.count("merge.test"); a != 1 || aaaa != 1 {
		t.Fatalf("A=%d AAAA=%d: the lookups were not merged", a, aaaa)
	}
}

func TestUnresolvableNameFails(t *testing.T) {
	dns := newFakeDNS(t, func(string, uint16) (int, []netip.Addr) { return 3, nil }) // NXDOMAIN
	c := dev()
	c.Resolvers, c.Hosts, c.Retries = []netip.AddrPort{dns.addr()}, noHosts(), -1
	h := newHarness(t, c)
	_, sub := newUA(t)
	sub.Endpoint = "http://nowhere.test:9/x"
	h.send(&Notification{ID: 1, Sub: sub})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Status != 0 || r.Attempts != 1 {
		t.Fatalf("%+v", r)
	}
	if a, _ := dns.count("nowhere.test"); a != 1 { // NXDOMAIN is final: no second server pass
		t.Fatalf("%d A queries", a)
	}
}

func TestLookupFallsBackToTheNextServer(t *testing.T) {
	svc := newPushSvc(t)
	live := newFakeDNS(t, answerWith(loopback[0]))
	dead, _ := net.ListenPacket("udp", "127.0.0.1:0") // nothing listens: ICMP port unreachable
	deadAddr := netip.MustParseAddrPort(dead.LocalAddr().String())
	dead.Close()
	mute := newFakeDNS(t, answerWith())
	mute.silent.Store(true) // reads, never answers
	c := dev()
	c.Resolvers, c.Hosts, c.DNSTimeout = []netip.AddrPort{deadAddr, mute.addr(), live.addr()}, noHosts(), 100*time.Millisecond
	h := newHarness(t, c)
	h.send(&Notification{ID: 1, Sub: named(t, svc, "http", "fallback.test", "/ok")})
	if r := h.result(); r.Outcome != OutcomeDelivered {
		t.Fatalf("%+v", r)
	}
}

func TestResolvedPrivateAddressesAreRefused(t *testing.T) {
	svc := newPushSvc(t)
	dns := newFakeDNS(t, answerWith(loopback[0], netip.MustParseAddr("10.1.2.3")))
	c := Config{AllowInsecure: true} // AllowPrivate off
	c.Resolvers, c.Hosts = []netip.AddrPort{dns.addr()}, noHosts()
	h := newHarness(t, c)
	h.send(&Notification{ID: 1, Sub: named(t, svc, "http", "rebind.test", "/ok")})
	r := h.result()
	if r.Outcome != OutcomeFailed || r.Status != 0 || r.Attempts != 1 { // not retried: it will not get better
		t.Fatalf("%+v", r)
	}
	if len(svc.got) != 0 {
		t.Fatal("the service behind the name was reached")
	}
}

func TestHostsFileNames(t *testing.T) {
	svc := newPushSvc(t)
	c := dev()
	c.Resolvers = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:1")} // would fail if asked
	c.Hosts = map[string][]netip.Addr{"Local.Test": loopback}
	h := newHarness(t, c)
	h.send(&Notification{ID: 1, Sub: named(t, svc, "http", "local.test", "/ok")})
	if r := h.result(); r.Outcome != OutcomeDelivered {
		t.Fatalf("%+v", r)
	}
}

func TestFallsThroughToTheNextAddress(t *testing.T) {
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("no IPv6 loopback")
	}
	svc := newPushSvcOn(t, l, false)
	// IPv4 is tried first, and nothing listens there on this port.
	dns := newFakeDNS(t, answerWith(loopback[0], netip.MustParseAddr("::1")))
	c := dev()
	c.Resolvers, c.Hosts = []netip.AddrPort{dns.addr()}, noHosts()
	h := newHarness(t, c)
	sub := svc.sub("/ok")
	sub.Endpoint = "http://dual.test:" + portOf(t, svc) + "/ok"
	h.send(&Notification{ID: 1, Sub: sub})
	if r := h.result(); r.Outcome != OutcomeDelivered {
		t.Fatalf("%+v", r)
	}
}

// ---- TLS ----

func tlsCfg(svc *pushSvc) Config {
	pool := x509.NewCertPool()
	pool.AddCert(svc.Certificate())
	c := dev()
	c.RootCAs, c.Hosts, c.AllowInsecure = pool, map[string][]netip.Addr{"example.com": loopback, "other.test": loopback}, false
	return c
}

func TestHTTPSDelivery(t *testing.T) {
	svc := newPushSvcOn(t, nil, true)
	h := newHarness(t, tlsCfg(svc))
	// by address (the test certificate names 127.0.0.1) and by name (and example.com)
	for i, sub := range []Subscription{svc.sub("/ok"), named(t, svc, "https", "example.com", "/ok")} {
		h.send(&Notification{ID: uint64(i), Sub: sub, Payload: []byte(`{"n":1}`), Topic: "t"})
		if r := h.result(); r.Outcome != OutcomeDelivered || r.Status != 201 {
			t.Fatalf("%d: %+v", i, r)
		}
	}
	reqs := svc.requests("/ok")
	if len(reqs) != 2 {
		t.Fatalf("%d requests", len(reqs))
	}
	for _, q := range reqs {
		if q.derr != nil || string(q.payload) != `{"n":1}` || q.header.Get("Topic") != "t" || q.header.Get("Connection") != "close" {
			t.Fatalf("%v %q %v", q.header, q.payload, q.derr)
		}
	}
}

func TestHTTPSRefusesWhatItCannotVerify(t *testing.T) {
	svc := newPushSvcOn(t, nil, true)
	c := tlsCfg(svc)
	c.Retries = -1
	h := newHarness(t, c)
	// a name the certificate does not cover
	h.send(&Notification{ID: 1, Sub: named(t, svc, "https", "other.test", "/ok")})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Status != 0 {
		t.Fatalf("wrong name: %+v", r)
	}
	// a server nobody trusts
	c.RootCAs = x509.NewCertPool()
	h2 := newHarness(t, c)
	h2.send(&Notification{ID: 2, Sub: svc.sub("/ok")})
	if r := h2.result(); r.Outcome != OutcomeFailed || r.Status != 0 {
		t.Fatalf("untrusted: %+v", r)
	}
	if len(svc.got) != 0 {
		t.Fatalf("%d requests got through an unverified connection", len(svc.got))
	}
}

func TestHTTPSToAPlainPortFails(t *testing.T) {
	svc := newPushSvc(t) // speaks plain HTTP
	c := dev()
	c.AllowInsecure, c.Retries = false, -1
	h := newHarness(t, c)
	sub := svc.sub("/ok")
	sub.Endpoint = "https://" + strings.TrimPrefix(svc.URL, "http://") + "/ok"
	h.send(&Notification{ID: 1, Sub: sub})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Status != 0 {
		t.Fatalf("%+v", r)
	}
}

// ---- what comes back ----

func TestResponseOddities(t *testing.T) {
	svc := newPushSvc(t)
	h := newHarness(t, dev())
	for _, path := range []string{"/early", "/body"} { // a 103 first; a big body we do not wait for
		h.send(&Notification{ID: 1, Sub: svc.sub(path)})
		if r := h.result(); r.Outcome != OutcomeDelivered || r.Status != 201 {
			t.Errorf("%s: %+v", path, r)
		}
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	svc := newPushSvc(t)
	c := dev()
	c.Retries = 1
	h := newHarness(t, c)
	start := time.Now()
	h.send(&Notification{ID: 1, Sub: svc.sub("/retry-after")})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Attempts != 2 || r.Status != 503 {
		t.Fatalf("%+v", r)
	}
	if d := time.Since(start); d < time.Second {
		t.Fatalf("retried after %v, Retry-After said 1s", d)
	}
}

func TestSlowServerTimesOut(t *testing.T) {
	svc := newPushSvc(t)
	c := dev()
	c.Timeout, c.Retries = 200*time.Millisecond, -1
	h := newHarness(t, c)
	start := time.Now()
	h.send(&Notification{ID: 1, Sub: svc.sub("/hold")})
	if r := h.result(); r.Outcome != OutcomeFailed || r.Status != 0 {
		t.Fatalf("%+v", r)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestNotHTTPFails(t *testing.T) {
	for name, reply := range map[string]string{
		"garbage":       "this is not http\r\n\r\n",
		"closes":        "",
		"huge head":     "HTTP/1.1 201 Created\r\n" + strings.Repeat("X-Pad: "+strings.Repeat("a", 1000)+"\r\n", 40),
		"bad status":    "HTTP/1.1 abc Nope\r\n\r\n",
		"status 99":     "HTTP/1.1 99 Nope\r\n\r\n",
		"http/2 answer": "HTTP/2 201\r\n\r\n",
	} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() {
					defer c.Close()
					buf := make([]byte, 4096)
					c.SetReadDeadline(time.Now().Add(time.Second))
					c.Read(buf)
					c.Write([]byte(reply))
				}()
			}
		}()
		cfg := dev()
		cfg.Retries = -1
		h := newHarness(t, cfg)
		_, sub := newUA(t)
		sub.Endpoint = "http://" + ln.Addr().String() + "/x"
		h.send(&Notification{ID: 1, Sub: sub})
		if r := h.result(); r.Outcome != OutcomeFailed || r.Status != 0 {
			t.Errorf("%s: %+v", name, r)
		}
		ln.Close()
	}
}

func TestParseHead(t *testing.T) {
	for _, tc := range []struct {
		in         string
		status     int
		retryAfter time.Duration
		ok, bad    bool
	}{
		{"HTTP/1.1 201 Created\r\nX: y\r\n\r\n", 201, 0, true, false},
		{"HTTP/1.1 429 Too Many\r\nretry-after: 7\r\n\r\nbody", 429, 7 * time.Second, true, false},
		{"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 410 Gone\r\n\r\n", 410, 0, true, false},
		{"HTTP/1.0 201 OK\r\n\r\n", 201, 0, true, false},
		{"HTTP/1.1 201 Cre", 0, 0, false, false}, // incomplete
		{"HTTP/1.1 503 x\r\nRetry-After: Wed, 21 Oct 2026 07:28:00 GMT\r\n\r\n", 503, 0, true, false},
		{"HTTP/1.1 503 x\r\nRetry-After: -3\r\n\r\n", 503, 0, true, false},
		{"junk\r\n\r\n", 0, 0, false, true},
		{"HTTP/1.1 700 x\r\n\r\n", 0, 0, false, true},
		{strings.Repeat("a", maxHead+1), 0, 0, false, true},
	} {
		b := []byte(tc.in)
		status, ra, ok, bad := parseHead(&b)
		if status != tc.status || ra != tc.retryAfter || ok != tc.ok || bad != tc.bad {
			t.Errorf("%q: got %d %v %v %v", tc.in, status, ra, ok, bad)
		}
	}
}

func TestEndpointParsing(t *testing.T) {
	for in, want := range map[string]target{
		"https://fcm.googleapis.com/fcm/send/abc?x=1": {tls: true, host: "fcm.googleapis.com", port: 443, hdr: "fcm.googleapis.com", uri: "/fcm/send/abc?x=1"},
		"http://127.0.0.1:8080/p":                     {host: "127.0.0.1", ip: netip.MustParseAddr("127.0.0.1"), port: 8080, hdr: "127.0.0.1:8080", uri: "/p"},
		"https://[2001:db8::1]:444":                   {tls: true, host: "2001:db8::1", ip: netip.MustParseAddr("2001:db8::1"), port: 444, hdr: "[2001:db8::1]:444", uri: "/"},
	} {
		got, err := parseTarget(in)
		if err != nil || got != want {
			t.Errorf("%s: %+v %v, want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"https://host:0/x", "https://host:99999/x", "https:///x", "ht tp://x"} {
		if _, err := parseTarget(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
