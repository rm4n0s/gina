// Server-Sent Events fed by a separate worker shard.
//
// The HTTP server runs on shards 0..N-1. A "clock" isolate lives alone on one
// extra shard (its own OS thread): it knows nothing about HTTP, it only keeps a
// list of subscribers and, once a second, sends the time to each of them through
// the cross-shard rings. Each subscriber is the connection isolate of an open
// /events request, which writes the message to its socket as an SSE event.
//
//	go run ./examples/sse -port 8080 -shards 2
//	curl -N localhost:8080/events      # or open http://localhost:8080/ in a browser
//
//	browser/curl ──GET /events──▶ conn isolate (shard 0..N-1)
//	                                │ 1. EventStream: answer text/event-stream, stay open
//	                                │ 2. subscribe ───────────────▶ clock isolate (shard N)
//	conn isolate ◀── every second: ghttp.SendEvent(time) ──────────┘
//	conn isolate ──▶ "data: 12:00:01\n\n" on the socket
//	conn closes ──▶ ghttp.TagStreamClosed ──▶ clock drops it
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"gina"
	ghttp "gina/extensions/http"
)

const (
	typeClock = 1 // application type; the http extension uses 200 and 201

	tagTick      = gina.TagUserBase     // timer: publish the time
	tagSubscribe = gina.TagUserBase + 1 // payload gina.Handle: a connection to feed
)

type clock struct{ subs []gina.Handle }

func clockInit(c *clock, ctx *gina.Ctx, _ []byte) gina.Effect {
	ctx.RegisterTimer(time.Second, tagTick)
	return gina.WaitMessage()
}

func clockHandler(c *clock, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagSubscribe:
		conn := *gina.PayloadAs[gina.Handle](m)
		c.subs = append(c.subs, conn)
		ghttp.SendEvent(ctx, conn, time.Now().Format("15:04:05")) // don't make a new viewer wait for the tick
	case ghttp.TagStreamClosed: // the client went away
		conn := *gina.PayloadAs[gina.Handle](m)
		for i, h := range c.subs {
			if h == conn {
				c.subs = append(c.subs[:i], c.subs[i+1:]...)
				break
			}
		}
	case tagTick:
		now := time.Now().Format("15:04:05")
		for _, h := range c.subs {
			ghttp.SendEvent(ctx, h, now)
		}
		ctx.RegisterTimer(time.Second, tagTick)
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

const page = `<!doctype html>
<meta charset="utf-8"><title>Gina SSE clock</title>
<body style="font:2rem monospace;text-align:center;margin-top:20vh">
<div id="t">connecting…</div>
<script>
  const es = new EventSource("/events");
  es.onmessage = e => t.textContent = e.data;
  es.onerror = () => t.textContent = "disconnected";
</script>`

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 2, "HTTP shards (the clock gets one more shard of its own)")
	pin := flag.Bool("pin", false, "pin shard threads to CPUs")
	flag.Parse()

	// The clock is the boot isolate of the last shard (index *shards), slot 0, generation 1.
	clockH := gina.MakeHandle(uint8(*shards), typeClock, 0, 1)

	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.Bytes(200, "text/html; charset=utf-8", []byte(page)) })
	r.GET("/events", func(c *ghttp.Context) {
		c.SetHeader("X-Accel-Buffering", "no")
		c.WriteString("retry: 2000\n\n") // sent first, verbatim
		// Turn this response into a stream; the clock is told when it ends.
		conn := c.EventStream(clockH)
		if conn != 0 {
			gina.Send(c.Gina(), clockH, tagSubscribe, &conn)
		}
	})
	srv := ghttp.New(ghttp.Config{Port: uint16(*port), ReusePort: *shards > 1}, r)

	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(typeClock, gina.TypeOptions{SlotCount: 1}, clockInit, clockHandler)},
		Shards: make([]gina.ShardSpec, *shards),
	}
	if err := srv.Install(&spec); err != nil { // http listener on shards 0..N-1 only
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}
	spec.Shards = append(spec.Shards, gina.ShardSpec{ // the clock's own shard: no listener
		Boot: []gina.SpawnSpec{{Type: typeClock, Group: gina.GroupRoot, Restart: gina.RestartPermanent}},
	})

	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen: %v)\n", err, srv.ListenErr())
		os.Exit(1)
	}
	defer sys.Close()
	fmt.Printf("%d http shard(s) + clock shard %d: http://localhost:%d/  (curl -N http://localhost:%d/events)\n", *shards, *shards, *port, *port)
	sys.Run(gina.RunOptions{Pin: *pin})
}
