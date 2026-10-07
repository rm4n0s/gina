// A WebSocket chat on Gina: ws:// and wss:// over HTTP/1.1, or over HTTP/2.
//
// The HTTP server runs on shards 0..N-1 and holds the connections. The chat room
// is a "hub" isolate alone on one extra shard (its own OS thread): it knows
// nothing about HTTP or WebSocket, only a list of Peers. A connection tells it
// when it opens and closes and hands it every line a user types; the hub pushes
// each line to every peer, on whichever shard and transport they live.
//
//	go run ./examples/websocket                          # ws://localhost:8080/
//	go run ./examples/websocket -tls -port 8443          # wss over HTTP/1.1, self-signed certificate
//	go run ./examples/websocket -tls -h2 -port 8443      # wss over HTTP/2 (RFC 8441); browsers: accept the certificate first
//	go run ./examples/websocket -h2                      # ws over h2c: for clients that speak it, not browsers
//	go run ./examples/websocket -shards 4 -pin           # four HTTP shard threads (+ the hub's)
//
//	browser A ──ws──▶ conn isolate (shard 0..N-1) ── join/line/leave ──▶ hub isolate (shard N)
//	browser B ◀──ws── conn isolate (any shard)    ◀── ws.PushText ──────┘
//
// Open the page in two tabs. Or from a shell, with any WebSocket client, e.g.
// websocat ws://localhost:8080/ws.
package main

import (
	"bytes"
	ctls "crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"unicode/utf8"

	"gina"
	ghttp "gina/extensions/http"
	"gina/extensions/http2"
	gtls "gina/extensions/tls"
	ws "gina/extensions/websocket"
)

const maxLine = 4096

const (
	typeHub = 1 // application type; the http extensions use 200..211

	tagJoin  = gina.TagUserBase     // payload ws.Peer
	tagLeave = gina.TagUserBase + 1 // payload ws.Peer
	tagLine  = gina.TagUserBase + 2 // data: one line of chat to broadcast (any length)
)

// ---- the hub ----

type hub struct{ peers []ws.Peer }

func hubHandler(h *hub, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagJoin:
		h.peers = append(h.peers, *gina.PayloadAs[ws.Peer](m))
	case tagLeave:
		p := *gina.PayloadAs[ws.Peer](m)
		for i, q := range h.peers {
			if q == p {
				h.peers = append(h.peers[:i], h.peers[i+1:]...)
				break
			}
		}
	case tagLine:
		line, err := ws.NewShared(ws.OpText, g.Data()) // one copy of the line, however many peers
		if err != nil {
			break
		}
		for _, p := range h.peers {
			ws.PushShared(g, p, line) // best effort: a peer that is gone or swamped just misses it
		}
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// ---- the page ----

const page = `<!doctype html>
<meta charset="utf-8"><title>Gina WebSocket chat</title>
<body style="font:16px system-ui;max-width:40rem;margin:2rem auto">
<h3>Gina WebSocket chat <small id="s" style="font-weight:normal;color:#888">connecting…</small></h3>
<pre id="log" style="height:20rem;overflow:auto;border:1px solid #ccc;padding:.5rem"></pre>
<form id="f"><input id="m" autofocus autocomplete="off" style="width:80%"> <button>send</button></form>
<script>
  const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
  const log = line => { document.getElementById("log").textContent += line + "\n"; };
  ws.onopen = () => document.getElementById("s").textContent = "connected";
  ws.onclose = e => document.getElementById("s").textContent = "closed (" + e.code + ")";
  ws.onmessage = e => log(e.data);
  f.onsubmit = e => { e.preventDefault(); if (m.value) ws.send(m.value); m.value = ""; };
</script>`

// ---- main ----

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 2, "HTTP shards (the hub gets one more shard of its own)")
	pin := flag.Bool("pin", false, "pin shard threads to CPUs")
	useTLS := flag.Bool("tls", false, "serve wss with a throwaway self-signed certificate for localhost")
	certFile := flag.String("cert", "", "PEM certificate chain (enables TLS)")
	keyFile := flag.String("key", "", "PEM private key for -cert")
	h2 := flag.Bool("h2", false, "speak HTTP/2 (WebSocket over extended CONNECT) instead of HTTP/1.1")
	flag.Parse()

	hubH := gina.MakeHandle(uint8(*shards), typeHub, 0, 1) // boot isolate of the last shard

	var guests atomic.Int64
	ep := ws.New(ws.Config{
		Subprotocols: []string{"chat"},
		OnOpen: func(c *ws.Conn) {
			c.Data = "guest-" + strconv.FormatInt(guests.Add(1), 10)
			p := c.Peer()
			gina.Send(c.Gina(), hubH, tagJoin, &p)
			c.SendText("welcome, " + c.Data.(string) + " (over " + proto(c) + ")")
		},
		OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
			if op != ws.OpText {
				c.Close(ws.CloseUnsupportedData, "text only")
				return
			}
			line := append([]byte(c.Data.(string)+": "), bytes.TrimSpace(data)...)
			for len(line) > maxLine { // lines are broadcast, so keep them modest; the engine would carry more
				_, n := utf8.DecodeLastRune(line)
				line = line[:len(line)-n]
			}
			c.Gina().SendRaw(hubH, tagLine, line)
		},
		OnClose: func(c *ws.Conn, code uint16, reason []byte) {
			p := c.Peer()
			gina.Send(c.Gina(), hubH, tagLeave, &p)
		},
	})

	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.Bytes(200, "text/html; charset=utf-8", []byte(page)) })
	r.GET("/ws", ep.Serve)
	r.GET("/stats", func(c *ghttp.Context) { c.String(200, fmt.Sprintf("connections=%d\n", ep.Conns())) })

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
		Types:  []gina.TypeDesc{gina.RegisterType(typeHub, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 1024}, nil, hubHandler)},
		Shards: make([]gina.ShardSpec, *shards),
	}
	var install func(*gina.SystemSpec) error
	var listenErr func() error
	var portOf func(int) uint16
	if *h2 {
		srv := http2.New(http2.Config{Port: uint16(*port), ReusePort: *shards > 1, TLS: tlsCfg,
			ExtendedConnect: true, ConnMailbox: ws.MailboxCapacity}, r)
		install, listenErr, portOf = srv.Install, srv.ListenErr, srv.Port
	} else {
		srv := ghttp.New(ghttp.Config{Port: uint16(*port), ReusePort: *shards > 1, TLS: tlsCfg,
			ConnMailbox: ws.MailboxCapacity}, r)
		install, listenErr, portOf = srv.Install, srv.ListenErr, srv.Port
	}
	if err := install(&spec); err != nil { // listeners on shards 0..N-1 only
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}
	spec.PoolSlots = max(spec.PoolSlots, 1<<15)       // room for the pushes queued in connection mailboxes
	spec.Shards = append(spec.Shards, gina.ShardSpec{ // the hub's own shard: no listener
		Boot: []gina.SpawnSpec{{Type: typeHub, Group: gina.GroupRoot, Restart: gina.RestartPermanent}},
	})

	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen: %v)\n", err, listenErr())
		os.Exit(1)
	}
	defer sys.Close()
	scheme := "ws"
	if tlsCfg != nil {
		scheme = "wss"
	}
	how := "HTTP/1.1"
	if *h2 {
		how = "HTTP/2"
	}
	fmt.Printf("%d %s shard(s) + hub shard %d serving %s on :%d — open %s://localhost:%d/ in two tabs\n",
		*shards, how, *shards, scheme, portOf(0), map[bool]string{false: "http", true: "https"}[tlsCfg != nil], *port)
	sys.Run(gina.RunOptions{Pin: *pin})
}

func proto(c *ws.Conn) string {
	if c.IsHTTP2() {
		return "HTTP/2"
	}
	return "HTTP/1.1"
}
