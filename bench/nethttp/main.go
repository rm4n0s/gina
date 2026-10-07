// A net/http server used ONLY as a comparison baseline for the Gina benchmarks.
// It is a separate Go module on purpose: it uses goroutines and net/http, which
// the Gina repository forbids. Same routes and response bodies as examples/httpserver.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"runtime"
	"time"
)

func main() {
	port := flag.Int("port", 8080, "port")
	useTLS := flag.Bool("tls", false, "serve HTTPS (TLS 1.3 only, HTTP/1.1 only unless -h2, ECDSA P-256 like Gina's self-signed cert)")
	h2 := flag.Bool("h2", false, "serve HTTP/2 only (h2 over TLS with -tls, h2c with prior knowledge otherwise) instead of HTTP/1.1")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %s!\n", r.PathValue("name"))
	})
	mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(b)
	})

	srv := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	if *useTLS {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			DNSNames: []string{"localhost"},
		}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		// SessionTicketsDisabled: Gina's TLS has no resumption, so don't charge net/http for issuing tickets.
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, SessionTicketsDisabled: true, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
		if !*h2 {
			srv.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){} // no HTTP/2: same protocol as Gina
		}
	}
	if *h2 { // HTTP/2 only, like extensions/http2
		var p http.Protocols
		if *useTLS {
			p.SetHTTP2(true)
		} else {
			p.SetUnencryptedHTTP2(true)
		}
		srv.Protocols = &p
	}
	fmt.Printf("net/http baseline on :%d tls=%v h2=%v GOMAXPROCS=%d\n", *port, *useTLS, *h2, runtime.GOMAXPROCS(0))
	var err error
	if *useTLS {
		err = srv.ListenAndServeTLS("", "")
	} else {
		err = srv.ListenAndServe()
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
