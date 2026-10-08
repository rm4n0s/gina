package sql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

func TestFakeAuthMethods(t *testing.T) {
	for _, auth := range []string{"trust", "cleartext", "md5", "scram"} {
		t.Run(auth, func(t *testing.T) {
			f := newFakePG(t, auth)
			f.script = func(string, []string) fakeResult { return rowsOf([]string{"v"}, []string{"ok"}) }
			h := newHarness(t, f.config())
			if v := scalar(t, h.mustQuery(`SELECT 1`)); v != "ok" {
				t.Fatalf("%v", v)
			}
		})
	}
}

func TestFakeWrongPassword(t *testing.T) {
	for _, auth := range []string{"cleartext", "md5", "scram"} {
		t.Run(auth, func(t *testing.T) {
			f := newFakePG(t, auth)
			cfg := f.config()
			cfg.Password = "nope"
			h := newHarness(t, cfg)
			r := h.query(`SELECT 1`)
			var pe *Error
			if !errors.Is(r.err, ErrConnect) || !errors.As(r.err, &pe) || pe.Code != "28P01" {
				t.Fatalf("%v", r.err)
			}
		})
	}
	t.Run("no password configured", func(t *testing.T) {
		f := newFakePG(t, "md5")
		cfg := f.config()
		cfg.Password = ""
		h := newHarness(t, cfg)
		if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrConnect) || !strings.Contains(r.err.Error(), "password") {
			t.Fatalf("%v", r.err)
		}
	})
}

func TestFakeScramImpostor(t *testing.T) {
	// A server that accepts any proof but cannot prove it knows the password.
	f := newFakePG(t, "scram-forged")
	h := newHarness(t, f.config())
	r := h.query(`SELECT 1`)
	if !errors.Is(r.err, ErrConnect) || !strings.Contains(r.err.Error(), "prove") {
		t.Fatalf("%v", r.err)
	}
}

func TestFakeStartupParams(t *testing.T) {
	f := newFakePG(t, "trust")
	cfg := f.config()
	cfg.ApplicationName = "orders"
	cfg.Params = map[string]string{"search_path": "app", "client_encoding": "LATIN1", "application_name": "x"}
	h := newHarness(t, cfg)
	h.mustQuery(`SELECT 1`)
	f.mu.Lock()
	p := f.startups[0]
	f.mu.Unlock()
	if p["user"] != "app" || p["database"] != "app" || p["application_name"] != "orders" || p["client_encoding"] != "UTF8" || p["search_path"] != "app" || p["DateStyle"] != "ISO" {
		t.Fatalf("%v", p)
	}
}

func TestFakeTLS(t *testing.T) {
	cert, err := gtls.SelfSigned("localhost")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)

	newTLSFake := func(auth string) *fakePG {
		f := newFakePG(t, auth)
		f.tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
		f.script = func(string, []string) fakeResult { return rowsOf([]string{"v"}, []string{"secure"}) }
		return f
	}

	t.Run("verified", func(t *testing.T) {
		f := newTLSFake("scram")
		cfg := f.config()
		cfg.TLS = &gtls.ClientConfig{ServerName: "localhost", RootCAs: roots}
		h := newHarness(t, cfg)
		for i := 0; i < 3; i++ { // several round trips through the TLS record layer
			if v := scalar(t, h.mustQuery(`SELECT 1`)); v != "secure" {
				t.Fatalf("%v", v)
			}
		}
		// a big result crosses several TLS records
		f.script = func(string, []string) fakeResult {
			rows := make([][]string, 3000)
			for i := range rows {
				rows[i] = []string{strings.Repeat("z", 100)}
			}
			return rowsOf([]string{"v"}, rows...)
		}
		if r := h.mustQuery(`SELECT many`); len(r.rows) != 3000 || r.rows[2999][0] != strings.Repeat("z", 100) {
			t.Fatalf("%d rows", len(r.rows))
		}
	})
	t.Run("wrong name", func(t *testing.T) {
		f := newTLSFake("trust")
		cfg := f.config()
		cfg.TLS = &gtls.ClientConfig{ServerName: "db.example.com", RootCAs: roots}
		h := newHarness(t, cfg)
		if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrConnect) {
			t.Fatalf("%v", r.err)
		}
	})
	t.Run("untrusted", func(t *testing.T) {
		f := newTLSFake("trust")
		cfg := f.config()
		cfg.TLS = &gtls.ClientConfig{ServerName: "localhost", RootCAs: x509.NewCertPool()}
		h := newHarness(t, cfg)
		if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrConnect) {
			t.Fatalf("%v", r.err)
		}
	})
	t.Run("server refuses", func(t *testing.T) {
		f := newFakePG(t, "trust") // no TLS: answers N
		cfg := f.config()
		cfg.TLS = &gtls.ClientConfig{ServerName: "localhost", RootCAs: roots}
		h := newHarness(t, cfg)
		r := h.query(`SELECT 1`)
		if !errors.Is(r.err, ErrConnect) || !strings.Contains(r.err.Error(), "TLS") {
			t.Fatalf("%v", r.err)
		}
		// nothing but the SSLRequest was sent: no password, no startup
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.startups) != 0 || f.sslSeen == 0 {
			t.Fatalf("startups %v, ssl requests %d", f.startups, f.sslSeen)
		}
	})
}

func TestFakeServerHangsUpMidResult(t *testing.T) {
	f := newFakePG(t, "trust")
	calls := 0
	f.script = func(sql string, _ []string) fakeResult {
		calls++
		if calls == 1 {
			r := rowsOf([]string{"v"}, []string{"1"}, []string{"2"}, []string{"3"}, []string{"4"})
			r.hangupAt = 2
			return r
		}
		return rowsOf([]string{"v"}, []string{"fine"})
	}
	h := newHarness(t, f.config())
	r := h.query(`SELECT many`)
	if !errors.Is(r.err, ErrConnLost) {
		t.Fatalf("%+v", r)
	}
	// the pool forgot that connection and makes another
	if v := scalar(t, h.mustQuery(`SELECT again`)); v != "fine" {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Opened != 2 || s.Open != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestFakeServerHangsUpBeforeAnswering(t *testing.T) {
	f := newFakePG(t, "trust")
	calls := 0
	f.script = func(string, []string) fakeResult {
		calls++
		if calls == 1 {
			return fakeResult{hangupBeforeReply: true}
		}
		return rowsOf([]string{"v"}, []string{"back"})
	}
	h := newHarness(t, f.config())
	if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrConnLost) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT 2`)); v != "back" {
		t.Fatalf("%v", v)
	}
}

func TestFakeProtocolViolations(t *testing.T) {
	f := newFakePG(t, "trust")
	bad := true
	f.script = func(string, []string) fakeResult {
		if bad {
			bad = false
			r := rowsOf([]string{"a"}, []string{"1"})
			r.badRow = true
			return r
		}
		return rowsOf([]string{"v"}, []string{"ok"})
	}
	h := newHarness(t, f.config())
	if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrProtocol) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT 2`)); v != "ok" {
		t.Fatalf("%v", v)
	}
}

func TestFakeCopyIsRefusedCleanly(t *testing.T) {
	f := newFakePG(t, "trust")
	f.script = func(sql string, _ []string) fakeResult {
		switch sql {
		case "COPY out":
			return fakeResult{copyOut: true}
		case "COPY in":
			return fakeResult{copyIn: true}
		}
		return rowsOf([]string{"v"}, []string{"ok"})
	}
	h := newHarness(t, f.config())
	if r := h.exec(`COPY out`); !errors.Is(r.err, ErrUnsupported) {
		t.Fatalf("%+v", r)
	}
	if r := h.exec(`COPY in`); !errors.Is(r.err, ErrUnsupported) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT 1`)); v != "ok" {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Opened != 1 {
		t.Fatalf("COPY should not have cost the connection: %+v", s)
	}
}

func TestFakeCancelPacket(t *testing.T) {
	f := newFakePG(t, "trust")
	f.script = func(sql string, _ []string) fakeResult {
		if sql == "slow" {
			// Answer only after the cancel has arrived, as an error, like PostgreSQL.
			for i := 0; i < 200; i++ {
				f.mu.Lock()
				n := len(f.cancels)
				f.mu.Unlock()
				if n > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			return fakeResult{hangupAt: -1, err: &Error{Severity: "ERROR", Code: "57014", Message: "canceling statement due to user request"}}
		}
		return rowsOf([]string{"v"}, []string{"ok"})
	}
	h := newHarness(t, f.config())
	id := newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: "slow", Timeout: 100 * time.Millisecond}) })
	if r := h.wait(id); !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.cancels) != 1 || f.cancels[0].pid != 1001 || len(f.cancels[0].key) != 4 {
		t.Fatalf("cancel requests: %+v", f.cancels)
	}
}

func TestFakeServerIgnoresCancel(t *testing.T) {
	f := newFakePG(t, "trust")
	f.script = func(sql string, _ []string) fakeResult {
		if sql == "stuck" {
			return fakeResult{silent: true}
		}
		return rowsOf([]string{"v"}, []string{"ok"})
	}
	cfg := f.config()
	cfg.CancelGrace = 300 * time.Millisecond
	h := newHarness(t, cfg)
	id := newID()
	start := time.Now()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: "stuck", Timeout: 100 * time.Millisecond}) })
	r := h.wait(id)
	if !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r)
	}
	if d := time.Since(start); d < 350*time.Millisecond || d > 3*time.Second {
		t.Fatalf("gave up after %v", d)
	}
	// the connection that did not react is dropped, and a fresh one works
	if v := scalar(t, h.mustQuery(`SELECT 1`)); v != "ok" {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Opened != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestFakeQueueFailsOldestOnConnectError(t *testing.T) {
	f := newFakePG(t, "trust")
	f.ln.Close() // nothing listens any more
	cfg := f.config()
	cfg.MaxOpenConns = 2
	h := newHarness(t, cfg)
	ids := []uint32{newID(), newID(), newID()}
	h.run(func(g *gina.Ctx) {
		for _, id := range ids {
			h.db.Query(g, &Request{ID: id, SQL: "SELECT 1"})
		}
	})
	for _, id := range ids {
		if r := h.wait(id); !errors.Is(r.err, ErrConnect) {
			t.Fatalf("%d: %+v", id, r)
		}
	}
	if s := h.db.Stats(); s.Open != 0 || s.Waiting != 0 || s.Failed < 3 {
		t.Fatalf("%+v", s)
	}
}

func TestFakeServerShutdownMessage(t *testing.T) {
	// An idle connection whose server hangs up is dropped from the pool without a request noticing.
	f := newFakePG(t, "trust")
	f.script = func(string, []string) fakeResult { return rowsOf([]string{"v"}, []string{"ok"}) }
	h := newHarness(t, f.config())
	h.mustQuery(`SELECT 1`)
	if f.liveConns() != 1 {
		t.Fatalf("%d", f.liveConns())
	}
	f.ln.Close()
	f.mu.Lock()
	f.mu.Unlock()
	// close the live connection from the server side
	closeAllFake(f)
	eventually(t, "the pool to drop the dead connection", func() bool { return h.db.Stats().Open == 0 })
}

// A server that keeps dropping connections: every request must still get exactly one
// answer, a request that had not started on a dying connection must not be lost, and
// the pool must end up with nothing leaked.
func TestFakeChurn(t *testing.T) {
	f := newFakePG(t, "trust")
	f.script = func(string, []string) fakeResult { return rowsOf([]string{"v"}, []string{"ok"}) }
	cfg := f.config()
	cfg.MaxOpenConns = 4
	h := newHarness(t, cfg)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				closeAllFake(f)
			}
		}
	}()

	const n = 1500
	ids := make([]uint32, n)
	for i := range ids {
		ids[i] = newID()
	}
	for start := 0; start < n; start += 50 {
		batch := ids[start : start+50]
		h.run(func(g *gina.Ctx) {
			for _, id := range batch {
				h.db.Query(g, &Request{ID: id, SQL: "SELECT 1"})
			}
		})
		time.Sleep(time.Millisecond)
	}
	ok, lost := 0, 0
	for _, id := range ids {
		r := h.wait(id)
		switch {
		case r.err == nil:
			ok++
		case errors.Is(r.err, ErrConnLost), errors.Is(r.err, ErrConnect):
			lost++
		default:
			t.Fatalf("request %d: %v", id, r.err)
		}
	}
	close(stop)
	<-done
	t.Logf("%d answered, %d lost a connection, %d requeued", ok, lost, h.db.Stats().Requeued)
	if ok < n/4 {
		t.Fatalf("only %d of %d succeeded", ok, n)
	}
	// every request was answered once; nothing else is pending
	for _, id := range ids[:20] {
		h.expectNone(id, 0)
	}
	eventually(t, "the pool to settle", func() bool {
		s := h.db.Stats()
		return s.InUse == 0 && s.Waiting == 0
	})
	if v := scalar(t, h.mustQuery(`SELECT 1`)); v != "ok" {
		t.Fatalf("%v", v)
	}
}

// Replies to a caller on the pool's own shard whose mailbox is full are kept and
// retried, not dropped.
func TestFakeReplyToBusyCaller(t *testing.T) {
	f := newFakePG(t, "trust")
	f.script = func(string, []string) fakeResult { return rowsOf([]string{"v"}, []string{"ok"}) }
	cfg := f.config()
	cfg.MaxOpenConns = 6
	db, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const typeTiny gina.TypeID = 2
	var got atomic.Int32
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 2)}
	spec.Types = append(spec.Types,
		gina.RegisterType(typeProbe, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 64}, nil, func(_ *probe, g *gina.Ctx, m *gina.Message) gina.Effect {
			if m.Tag == gina.TagShutdown {
				return gina.Done()
			}
			if m.Tag == tagDo {
				tiny := gina.MakeHandle(1, typeTiny, 0, 1)
				for i := 0; i < 60; i++ {
					db.Query(g, &Request{ID: uint32(i + 1), SQL: "SELECT 1", ReplyTo: tiny})
				}
			}
			return gina.WaitMessage()
		}),
		gina.RegisterType(typeTiny, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 1}, nil, func(_ *probe, g *gina.Ctx, m *gina.Message) gina.Effect {
			if m.Tag == gina.TagShutdown {
				return gina.Done()
			}
			if m.Tag == TagReply {
				if rep, err := Decode(g, m); err == nil && rep.Kind == ReplyRows && !rep.More {
					got.Add(1)
				}
			}
			return gina.WaitMessage()
		}))
	spec.Shards[0].Boot = append(spec.Shards[0].Boot, gina.SpawnSpec{Type: typeProbe, Group: gina.GroupRoot, Restart: gina.RestartPermanent})
	spec.Shards[1].Boot = append(spec.Shards[1].Boot, gina.SpawnSpec{Type: typeTiny, Group: gina.GroupRoot, Restart: gina.RestartPermanent})
	if err := db.Install(&spec); err != nil {
		t.Fatal(err)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sys.Start(gina.RunOptions{})
	t.Cleanup(func() { sys.Stop(); sys.Wait(); sys.Close() })
	sys.SendExternal(gina.MakeHandle(0, typeProbe, 0, 1), tagDo, []byte{0, 0, 0, 0})
	eventually(t, "all replies", func() bool { return got.Load() == 60 })
	t.Logf("%d replies were redelivered", db.Stats().Redelivered)
}
