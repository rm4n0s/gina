// An HTTP server on Gina.
//
//	go run ./examples/httpserver -port 8080                      # 1 process, 1 shard
//	go run ./examples/httpserver -port 8080 -shards 8 -pin      # 1 process, 8 shard THREADS (one per core), SO_REUSEPORT listeners: Tina's model
//	go run ./examples/httpserver -port 8443 -tls                 # HTTPS (TLS 1.3), self-signed certificate
//	go run ./examples/httpserver -port 8443 -cert c.pem -key k.pem
//
// With -shards N every shard thread binds the port with SO_REUSEPORT, so the kernel
// spreads connections across cores.
package main

import (
	ctls "crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 1, "shards per process: each runs on its own OS thread with its own SO_REUSEPORT listener")
	pin := flag.Bool("pin", false, "pin each shard thread to its own CPU")
	maxConns := flag.Int("maxconns", 4096, "max connections per shard")
	useTLS := flag.Bool("tls", false, "serve HTTPS (TLS 1.3) with a throwaway self-signed certificate for localhost")
	certFile := flag.String("cert", "", "PEM certificate chain (enables HTTPS)")
	keyFile := flag.String("key", "", "PEM private key for -cert")
	flag.Parse()

	pid := os.Getpid()

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

	r := ghttp.NewRouter()
	srv := ghttp.New(ghttp.Config{
		Port:      uint16(*port),
		ReusePort: *shards > 1,
		MaxConns:  *maxConns,
		TLS:       tlsCfg,
	}, r)

	who := func(c *ghttp.Context) string {
		return "pid " + strconv.Itoa(pid) + " shard " + strconv.Itoa(int(c.Gina().ShardID()))
	}
	r.GET("/", func(c *ghttp.Context) { c.String(200, "Hello from Gina ("+who(c)+")\n") })
	r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "Hello, "+c.Param("name")+"!\n") })
	r.GET("/json", func(c *ghttp.Context) {
		c.JSON(200, fmt.Sprintf(`{"pid":%d,"shard":%d}`, pid, c.Gina().ShardID()))
	})
	r.POST("/echo", func(c *ghttp.Context) { c.Bytes(200, "application/octet-stream", c.Req.Body) })
	r.GET("/stats", func(c *ghttp.Context) {
		c.String(200, fmt.Sprintf("pid=%d requests=%d conns=%d rejected=%d\n", pid, srv.Requests(), srv.Conns(), srv.Rejected()))
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
	scheme := "http"
	if tlsCfg != nil {
		scheme = "https"
	}
	for i := 0; i < *shards; i++ {
		fmt.Printf("pid %d shard %d serving %s on :%d (reuseport=%v)\n", pid, i, scheme, srv.Port(i), *shards > 1)
	}
	sys.Run(gina.RunOptions{Pin: *pin}) // one OS thread per shard; serves until killed
}
