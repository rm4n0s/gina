// How to set up Gina shards.
//
// A System is a set of shards, each running on its own OS thread (optionally pinned
// to a core). A shard owns its isolates, message pool, timers and sockets; isolates
// on different shards message each other through lock-free rings, using nothing but
// a Handle. With ReusePort every shard binds its own listener on the same port and
// the kernel balances connections across the shard threads.
//
// This example boots one "node" isolate per shard. Shard 0's node launches a token
// that travels around the ring of shards and reports the lap time; the HTTP server
// shows which shard answered each request.
//
//	go run ./examples/shards                  # 2 shard threads
//	go run ./examples/shards -shards 4 -pin   # 4 shard threads, one per CPU
//	curl localhost:8080/                      # repeat: answers come from different shards
package main

import (
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
)

const (
	typeNode = 1 // application isolate types; the http extension uses 200 and 201

	tagRound = gina.TagUserBase     // timer: the origin node launches a token
	tagToken = gina.TagUserBase + 1 // a token arrives from the previous shard
)

// nodeArgs and token travel in fixed-size, pointer-free structs (no padding).
type nodeArgs struct {
	Next   gina.Handle // the node on the next shard
	Origin uint64      // 1 on shard 0, which launches the tokens
}
type token struct {
	Start uint64 // wall clock when launched, ns
	Hops  uint64
}

type node struct {
	next   gina.Handle
	origin bool
	seen   bool
}

var rounds, lastRoundNs atomic.Uint64

func nodeInit(n *node, ctx *gina.Ctx, raw []byte) gina.Effect {
	a := gina.ArgsAs[nodeArgs](raw)
	n.next, n.origin = a.Next, a.Origin == 1
	if n.origin {
		ctx.RegisterTimer(100*time.Millisecond, tagRound) // let every shard finish booting first
	}
	return gina.WaitMessage()
}

func nodeHandler(n *node, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagRound:
		gina.Send(ctx, n.next, tagToken, &token{Start: uint64(time.Now().UnixNano()), Hops: 1})
	case tagToken:
		t := *gina.PayloadAs[token](m)
		if n.origin { // back where it started: one full lap
			lastRoundNs.Store(uint64(time.Now().UnixNano()) - t.Start)
			if rounds.Add(1) == 1 {
				fmt.Printf("token made %d hops around the ring in %v\n", t.Hops, time.Duration(lastRoundNs.Load()))
			}
			ctx.RegisterTimer(time.Second, tagRound)
			break
		}
		if !n.seen {
			n.seen = true
			fmt.Printf("shard %d: token arrived after %d hops\n", ctx.ShardID(), t.Hops)
		}
		t.Hops++
		gina.Send(ctx, n.next, tagToken, &t)
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 2, "shards (OS threads)")
	pin := flag.Bool("pin", false, "pin shard threads to CPUs")
	flag.Parse()

	// 1. The HTTP server: with several shards every listener must bind the same
	//    port, which needs ReusePort.
	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) {
		c.String(200, fmt.Sprintf("shard %d\nring: %d laps, last %v\n",
			c.Gina().ShardID(), rounds.Load(), time.Duration(lastRoundNs.Load())))
	})
	srv := ghttp.New(ghttp.Config{Port: uint16(*port), ReusePort: *shards > 1}, r)

	// 2. The spec: one ShardSpec per shard, each with its own boot isolates. The
	//    shards form a ring, so shard i's node sends to shard (i+1)%N. A handle is
	//    shard | type | slot | generation; boot isolates sit at slot 0, generation 1.
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(typeNode, gina.TypeOptions{SlotCount: 2}, nodeInit, nodeHandler)},
		Shards: make([]gina.ShardSpec, *shards),
	}
	for i := range spec.Shards {
		next := gina.MakeHandle(uint8((i+1)%*shards), typeNode, 0, 1)
		origin := uint64(0)
		if i == 0 {
			origin = 1
		}
		spec.Shards[i].Boot = []gina.SpawnSpec{
			gina.WithArgs(gina.SpawnSpec{Type: typeNode, Group: gina.GroupRoot, Restart: gina.RestartPermanent}, &nodeArgs{Next: next, Origin: origin}),
		}
	}
	if err := srv.Install(&spec); err != nil { // adds the http types and a listener to every shard
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}

	// 3. Create the System (this boots every shard and binds the listeners) and run
	//    it: one OS thread per shard, serving until StopSystem or Stop.
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen: %v)\n", err, srv.ListenErr())
		os.Exit(1)
	}
	defer sys.Close()
	fmt.Printf("%d shard thread(s) serving http://localhost:%d/\n", *shards, *port)
	sys.Run(gina.RunOptions{Pin: *pin})
}
