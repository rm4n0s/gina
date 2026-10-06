// An HTTP server on Gina.
//
//	go run ./examples/httpserver -port 8080                      # 1 process, 1 shard
//	go run ./examples/httpserver -port 8080 -shards 4            # 1 process, 4 shards, SO_REUSEPORT listeners
//	go run ./examples/httpserver -port 8080 -workers 8 -pin      # 8 worker processes, same port, one per core
//
// -workers uses gina.Prefork: the binary re-executes itself and every worker binds
// the port with SO_REUSEPORT, so the kernel spreads connections across cores.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"gina"
	ghttp "gina/extensions/http"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	workers := flag.Int("workers", 1, "worker processes (Prefork); each binds the port with SO_REUSEPORT")
	shards := flag.Int("shards", 1, "shards per process (each has its own SO_REUSEPORT listener)")
	pin := flag.Bool("pin", false, "pin each worker process to a CPU")
	maxConns := flag.Int("maxconns", 4096, "max connections per shard")
	flag.Parse()

	worker := 0
	if *workers > 1 {
		worker = gina.Prefork(*workers, gina.PreforkOptions{Pin: *pin})
	}
	pid := os.Getpid()

	r := ghttp.NewRouter()
	srv := ghttp.New(ghttp.Config{
		Port:      uint16(*port),
		ReusePort: *workers > 1 || *shards > 1,
		MaxConns:  *maxConns,
	}, r)

	who := func(c *ghttp.Context) string {
		return "worker " + strconv.Itoa(worker) + " pid " + strconv.Itoa(pid) + " shard " + strconv.Itoa(int(c.Gina().ShardID()))
	}
	r.GET("/", func(c *ghttp.Context) { c.String(200, "Hello from Gina ("+who(c)+")\n") })
	r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "Hello, "+c.Param("name")+"!\n") })
	r.GET("/json", func(c *ghttp.Context) {
		c.JSON(200, fmt.Sprintf(`{"worker":%d,"pid":%d,"shard":%d}`, worker, pid, c.Gina().ShardID()))
	})
	r.POST("/echo", func(c *ghttp.Context) { c.Bytes(200, "application/octet-stream", c.Req.Body) })
	r.GET("/stats", func(c *ghttp.Context) {
		c.String(200, fmt.Sprintf("worker=%d pid=%d requests=%d conns=%d rejected=%d\n", worker, pid, srv.Requests(), srv.Conns(), srv.Rejected()))
	})
	r.GET("/panic", func(c *ghttp.Context) { panic("demo: handler panics become 500s") })

	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, *shards)}
	if err := srv.Install(&spec); err != nil {
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen: %v)\n", err, srv.ListenErr())
		os.Exit(1)
	}
	for i := 0; i < *shards; i++ {
		fmt.Printf("worker %d pid %d shard %d listening on :%d (reuseport=%v)\n", worker, pid, i, srv.Port(i), *workers > 1 || *shards > 1)
	}
	sys.RunUntilIdle(1 << 62) // serves until killed
}
