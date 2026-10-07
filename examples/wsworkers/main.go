// Two workers and a browser, talking over one WebSocket.
//
//	go run ./examples/wsworkers                  # then open http://localhost:8080/
//	go run ./examples/wsworkers -tls -port 8443  # wss, self-signed certificate
//	go run ./examples/wsworkers -tls -h2 -port 8443
//
// The HTTP server runs on shards 0..N-1 and holds the browsers' connections. The
// two workers are plain isolates, each alone on a shard of its own (its own OS
// thread); they know nothing about HTTP or WebSocket, only the Peers of the
// connections they were told about.
//
//	clock worker  (shard N)    every second pushes the time to every browser
//	greeter worker (shard N+1) prints "hello" to this console when a browser's button is pressed
//
//	browser ──ws──▶ connection isolate ── subscribe / unsubscribe ──▶ clock worker
//	browser ◀──ws── connection isolate ◀── ws.PushShared("time: ...") ┘
//	browser ──"hello"──▶ connection isolate ── hello ──▶ greeter worker ──▶ prints "hello"
//	browser ◀──ws── connection isolate ◀── ws.PushText("ack: ...") ───────┘
package main

import (
	ctls "crypto/tls"
	"flag"
	"fmt"
	"os"
	"time"

	"gina"
	ghttp "gina/extensions/http"
	"gina/extensions/http2"
	gtls "gina/extensions/tls"
	ws "gina/extensions/websocket"
)

const (
	typeClock   = 1 // application types; the http extensions use 200..211
	typeGreeter = 2

	tagTick        = gina.TagUserBase     // timer: the clock's second is up
	tagSubscribe   = gina.TagUserBase + 1 // payload ws.Peer: start sending the time here
	tagUnsubscribe = gina.TagUserBase + 2 // payload ws.Peer
	tagHello       = gina.TagUserBase + 3 // payload ws.Peer: this browser pressed the button
)

// ---- worker 1: the clock ----

type clock struct{ subs []ws.Peer }

func clockInit(c *clock, g *gina.Ctx, _ []byte) gina.Effect {
	g.RegisterTimer(time.Second, tagTick)
	return gina.WaitMessage()
}

func clockHandler(c *clock, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagSubscribe:
		p := *gina.PayloadAs[ws.Peer](m)
		c.subs = append(c.subs, p)
		ws.PushText(g, p, "time: "+time.Now().Format("15:04:05")) // don't make a new browser wait for the tick
	case tagUnsubscribe:
		p := *gina.PayloadAs[ws.Peer](m)
		for i, q := range c.subs {
			if q == p {
				c.subs = append(c.subs[:i], c.subs[i+1:]...)
				break
			}
		}
	case tagTick:
		if len(c.subs) > 0 {
			msg, _ := ws.NewShared(ws.OpText, []byte("time: "+time.Now().Format("15:04:05"))) // built once, sent to all
			for _, p := range c.subs {
				ws.PushShared(g, p, msg)
			}
		}
		g.RegisterTimer(time.Second, tagTick)
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// ---- worker 2: the greeter ----

type greeter struct{}

func greeterHandler(_ *greeter, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagHello:
		fmt.Println("hello") // runs on the greeter's own shard thread
		ws.PushText(g, *gina.PayloadAs[ws.Peer](m), "ack: the server printed hello")
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// ---- the page ----

const page = `<!doctype html>
<meta charset="utf-8"><title>Gina workers</title>
<body style="font:16px system-ui;max-width:32rem;margin:3rem auto;text-align:center">
<div id="time" style="font:700 3rem monospace">--:--:--</div>
<p><button id="b" disabled style="font-size:1.2rem;padding:.5rem 1.5rem">Say hello</button></p>
<p id="note" style="color:#555;min-height:1.5em"></p>
<small id="s" style="color:#888">connecting…</small>
<script>
  const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
  ws.onopen = () => { s.textContent = "connected"; b.disabled = false; };
  ws.onclose = e => { s.textContent = "closed (" + e.code + ")"; b.disabled = true; };
  ws.onmessage = e => {
    if (e.data.startsWith("time: ")) time.textContent = e.data.slice(6);
    else if (e.data.startsWith("ack: ")) note.textContent = e.data.slice(5);
  };
  b.onclick = () => ws.send("hello");
</script>`

// ---- main ----

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 1, "HTTP shards (each worker gets a shard of its own after them)")
	pin := flag.Bool("pin", false, "pin shard threads to CPUs")
	useTLS := flag.Bool("tls", false, "serve wss with a throwaway self-signed certificate for localhost")
	certFile := flag.String("cert", "", "PEM certificate chain (enables TLS)")
	keyFile := flag.String("key", "", "PEM private key for -cert")
	h2 := flag.Bool("h2", false, "speak HTTP/2 (WebSocket over extended CONNECT) instead of HTTP/1.1")
	flag.Parse()

	// each worker is the boot isolate of its own shard: slot 0, generation 1
	clockH := gina.MakeHandle(uint8(*shards), typeClock, 0, 1)
	greeterH := gina.MakeHandle(uint8(*shards+1), typeGreeter, 0, 1)

	ep := ws.New(ws.Config{
		OnOpen: func(c *ws.Conn) {
			p := c.Peer()
			gina.Send(c.Gina(), clockH, tagSubscribe, &p)
		},
		OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
			if string(data) == "hello" {
				p := c.Peer()
				gina.Send(c.Gina(), greeterH, tagHello, &p)
			}
		},
		OnClose: func(c *ws.Conn, _ uint16, _ []byte) {
			p := c.Peer()
			gina.Send(c.Gina(), clockH, tagUnsubscribe, &p)
		},
	})
	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.Bytes(200, "text/html; charset=utf-8", []byte(page)) })
	r.GET("/ws", ep.Serve)

	var tlsCfg *gtls.Config
	if *useTLS || *certFile != "" {
		var cert ctls.Certificate
		var err error
		if *certFile != "" {
			cert, err = gtls.LoadX509KeyPair(*certFile, *keyFile)
		} else {
			cert, err = gtls.SelfSigned("localhost", "127.0.0.1") // dev only
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "tls:", err)
			os.Exit(1)
		}
		tlsCfg = &gtls.Config{Certificates: []ctls.Certificate{cert}}
	}

	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{
			gina.RegisterType(typeClock, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 256}, clockInit, clockHandler),
			gina.RegisterType(typeGreeter, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 64}, nil, greeterHandler),
		},
		Shards: make([]gina.ShardSpec, *shards),
	}
	var install func(*gina.SystemSpec) error
	var listenErr func() error
	if *h2 {
		srv := http2.New(http2.Config{Port: uint16(*port), ReusePort: *shards > 1, TLS: tlsCfg,
			ExtendedConnect: true, ConnMailbox: ws.MailboxCapacity}, r)
		install, listenErr = srv.Install, srv.ListenErr
	} else {
		srv := ghttp.New(ghttp.Config{Port: uint16(*port), ReusePort: *shards > 1, TLS: tlsCfg,
			ConnMailbox: ws.MailboxCapacity}, r)
		install, listenErr = srv.Install, srv.ListenErr
	}
	if err := install(&spec); err != nil { // listeners on shards 0..N-1 only
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}
	spec.Shards = append(spec.Shards,
		gina.ShardSpec{Boot: []gina.SpawnSpec{{Type: typeClock, Group: gina.GroupRoot, Restart: gina.RestartPermanent}}},
		gina.ShardSpec{Boot: []gina.SpawnSpec{{Type: typeGreeter, Group: gina.GroupRoot, Restart: gina.RestartPermanent}}},
	)

	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen: %v)\n", err, listenErr())
		os.Exit(1)
	}
	defer sys.Close()
	scheme := "http"
	if tlsCfg != nil {
		scheme = "https"
	}
	fmt.Printf("%d HTTP shard(s), clock worker on shard %d, greeter worker on shard %d: open %s://localhost:%d/\n",
		*shards, *shards, *shards+1, scheme, *port)
	sys.Run(gina.RunOptions{Pin: *pin})
}
