// An HTTP/2 server on Gina. The routes are an extensions/http Router, the same
// type the HTTP/1.1 examples use.
//
//	go run ./examples/http2 -port 8080                       # h2c (cleartext, prior knowledge)
//	go run ./examples/http2 -port 8443 -tls                  # h2 over TLS 1.3, self-signed certificate
//	go run ./examples/http2 -port 8443 -cert c.pem -key k.pem
//	go run ./examples/http2 -port 8443 -tls -shards 8 -pin   # 8 shard threads, SO_REUSEPORT listeners
//
// Try it:
//
//	curl --http2-prior-knowledge http://localhost:8080/
//	curl -k --http2 https://localhost:8443/                  # -k: the certificate is self-signed
//
// The port speaks HTTP/2 only: a browser works over -tls (it negotiates "h2"),
// a plain `curl http://...` does not (without --http2-prior-knowledge it speaks
// HTTP/1.1, which this port does not serve).
package main

import (
	ctls "crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"

	"gina"
	ghttp "gina/extensions/http"
	"gina/extensions/http2"
	gtls "gina/extensions/tls"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 1, "shards per process: each runs on its own OS thread with its own SO_REUSEPORT listener")
	pin := flag.Bool("pin", false, "pin each shard thread to its own CPU")
	maxConns := flag.Int("maxconns", 4096, "max connections per shard")
	streams := flag.Int("streams", 100, "max concurrent streams per connection")
	useTLS := flag.Bool("tls", false, "serve h2 over TLS 1.3 with a throwaway self-signed certificate for localhost")
	certFile := flag.String("cert", "", "PEM certificate chain (enables TLS)")
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
		tlsCfg = &gtls.Config{Certificates: []ctls.Certificate{cert}} // ALPN is forced to "h2"
	}

	r := ghttp.NewRouter()
	srv := http2.New(http2.Config{
		Port:                 uint16(*port),
		ReusePort:            *shards > 1,
		MaxConns:             *maxConns,
		MaxConcurrentStreams: *streams,
		TLS:                  tlsCfg,
	}, r)

	who := func(c *ghttp.Context) string {
		return "pid " + strconv.Itoa(pid) + " shard " + strconv.Itoa(int(c.Gina().ShardID()))
	}
	r.GET("/", func(c *ghttp.Context) { c.String(200, "Hello over HTTP/2 from Gina ("+who(c)+")\n") })
	r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "Hello, "+c.Param("name")+"!\n") })
	r.GET("/json", func(c *ghttp.Context) {
		c.JSON(200, fmt.Sprintf(`{"pid":%d,"shard":%d}`, pid, c.Gina().ShardID()))
	})
	r.POST("/echo", func(c *ghttp.Context) { c.Bytes(200, "application/octet-stream", c.Req.Body) })
	r.GET("/tls", func(c *ghttp.Context) { // what this connection negotiated
		info, ok := c.TLS()
		if !ok {
			c.String(200, "cleartext h2c\n")
			return
		}
		c.String(200, fmt.Sprintf("TLS 1.3\ncipher: %s\nalpn:   %s\nsni:    %q\n", info.CipherName(), info.ALPN, info.ServerName))
	})
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
	mode := "h2c"
	if tlsCfg != nil {
		mode = "h2 over TLS"
	}
	for i := 0; i < *shards; i++ {
		fmt.Printf("pid %d shard %d serving %s on :%d (reuseport=%v)\n", pid, i, mode, srv.Port(i), *shards > 1)
	}
	sys.Run(gina.RunOptions{Pin: *pin}) // one OS thread per shard; serves until killed
}
