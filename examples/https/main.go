// An HTTPS server on Gina (TLS 1.3), with a plain-HTTP port that redirects to it.
//
//	go run ./examples/https                                  # https://localhost:8443, http://localhost:8080 -> 8443
//	go run ./examples/https -cert cert.pem -key key.pem      # your own certificate
//	go run ./examples/https -http-port 0                     # HTTPS only
//
// Without -cert/-key it creates a throwaway self-signed certificate for localhost,
// so browsers and curl need -k / --insecure (or to trust it).
//
// Both servers live in one gina.System (one shard thread here): an isolate per
// connection, plus a listener isolate per server. TLS runs inside the connection
// isolates (extensions/tls).
package main

import (
	ctls "crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gina"
	ghttp "gina/extensions/http"
	gtls "gina/extensions/tls"
)

func main() {
	httpsPort := flag.Int("https-port", 8443, "HTTPS port")
	httpPort := flag.Int("http-port", 8080, "plain HTTP port that redirects to HTTPS (0 disables)")
	certFile := flag.String("cert", "", "PEM certificate chain (default: self-signed for localhost)")
	keyFile := flag.String("key", "", "PEM private key for -cert")
	flag.Parse()

	// 1. A certificate. Prefer ECDSA or Ed25519 keys: the handshake runs on the
	//    shard's thread and RSA signatures are roughly ten times slower.
	var cert ctls.Certificate
	var err error
	if *certFile != "" {
		cert, err = gtls.LoadX509KeyPair(*certFile, *keyFile)
	} else {
		cert, err = gtls.SelfSigned("localhost", "127.0.0.1")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "certificate:", err)
		os.Exit(1)
	}

	// 2. Routes. c.TLS() reports what this connection negotiated.
	r := ghttp.NewRouter()
	hsts := func(c *ghttp.Context) { c.SetHeader("Strict-Transport-Security", "max-age=31536000") }

	r.GET("/", func(c *ghttp.Context) {
		hsts(c)
		info, _ := c.TLS()
		c.String(200, fmt.Sprintf("Hello over TLS 1.3!\ncipher: %s\nalpn:   %s\nsni:    %q\n", info.CipherName(), info.ALPN, info.ServerName))
	})
	r.GET("/hello/:name", func(c *ghttp.Context) {
		hsts(c)
		c.String(200, "Hello, "+c.Param("name")+"!\n")
	})
	r.GET("/tls", func(c *ghttp.Context) {
		hsts(c)
		info, _ := c.TLS()
		c.JSON(200, fmt.Sprintf(`{"version":"TLS1.3","cipher":%q,"alpn":%q,"sni":%q}`, info.CipherName(), info.ALPN, info.ServerName))
	})
	r.POST("/echo", func(c *ghttp.Context) { hsts(c); c.Bytes(200, "application/octet-stream", c.Req.Body) })

	// 3. The HTTPS server.
	https := ghttp.New(ghttp.Config{
		Port: uint16(*httpsPort),
		TLS:  &gtls.Config{Certificates: []ctls.Certificate{cert}}, // ALPN defaults to http/1.1
	}, r)

	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 1)}
	if err := https.Install(&spec); err != nil {
		fmt.Fprintln(os.Stderr, "https:", err)
		os.Exit(1)
	}

	// 4. Optional: a plain HTTP server whose only job is to redirect. A second
	//    server in the same system just needs its own isolate type ids.
	if *httpPort != 0 {
		redirect := ghttp.NewRouter()
		redirect.NotFound = func(c *ghttp.Context) {
			host := string(c.Req.Header("Host"))
			if i := strings.LastIndexByte(host, ':'); i >= 0 {
				host = host[:i]
			}
			if host == "" {
				host = "localhost"
			}
			target := "https://" + host
			if *httpsPort != 443 {
				target += ":" + strconv.Itoa(*httpsPort)
			}
			c.Redirect(308, target+string(c.Req.Target))
		}
		plain := ghttp.New(ghttp.Config{Port: uint16(*httpPort), TypeIDBase: 202}, redirect)
		if err := plain.Install(&spec); err != nil {
			fmt.Fprintln(os.Stderr, "http:", err)
			os.Exit(1)
		}
	}

	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen error: %v)\n", err, https.ListenErr())
		os.Exit(1)
	}
	fmt.Printf("HTTPS  https://localhost:%d/\n", https.Port(0))
	if *httpPort != 0 {
		fmt.Printf("HTTP   http://localhost:%d/  (redirects to HTTPS)\n", *httpPort)
	}
	sys.Run(gina.RunOptions{}) // serves until killed
}
