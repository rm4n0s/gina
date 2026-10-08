// Web Push: a browser subscribes, the server sends it notifications.
//
//	go run ./examples/webpush                  # http://localhost:8080  (service workers work on localhost without TLS)
//	go run ./examples/webpush -tls -port 8443  # https://localhost:8443 (a self-signed certificate: the browser must trust it)
//
// Open the page, press "Enable notifications" and allow them, then press "Send a
// notification" (or `curl -d 'hello' localhost:8080/notify`): the browser shows it
// even if you close the tab, as long as the browser itself is running.
//
// Web Push needs a secure context, so a page served from a LAN address over plain
// http will not offer it, and Chrome will not register a service worker on a
// certificate it does not trust. Push services (Google, Mozilla, Apple) are
// reached over the internet, so this needs network access.
//
// The VAPID key pair is kept in vapid.key: a subscription belongs to the public
// key it was made with, so deleting the file orphans every subscriber.
//
//	browser ──POST /subscribe──▶ HTTP shard (0..N-1) ──▶ hub isolate ─┐   (shard N)
//	browser ──POST /notify─────▶ HTTP shard (0..N-1) ──▶ hub isolate  │ webpush.Send per subscriber
//	                                                       ▲          ▼
//	                                  TagResult (Gone: forget it)   sender isolate ─▶ delivery isolates ─▶ push service ─▶ browser
//
// The hub keeps the subscriptions (in memory, so a restart forgets them: a real
// application stores them) and shares shard N with the sender.
package main

import (
	ctls "crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
	gtls "github.com/rm4n0s/gina/extensions/tls"
	"github.com/rm4n0s/gina/extensions/webpush"
)

const (
	typeHub = 1 // application type; http uses 200-201 and webpush 220

	tagSubscribe   = gina.TagUserBase + 1 // data: the subscription JSON from the browser
	tagUnsubscribe = gina.TagUserBase + 2 // data: the endpoint
	tagBroadcast   = gina.TagUserBase + 3 // data: the payload to push to everyone
)

// ---- the hub: owns the subscriptions ----

type sub struct {
	id uint64
	s  webpush.Subscription
}

type hub struct {
	subs []sub
	next uint64
}

func hubHandler(wp *webpush.WebPush) gina.Handler[hub] {
	return func(h *hub, ctx *gina.Ctx, m *gina.Message) gina.Effect { return h.handle(wp, ctx, m) }
}

func (h *hub) handle(wp *webpush.WebPush, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagSubscribe:
		s, err := webpush.ParseSubscription(ctx.Data())
		if err != nil {
			break
		}
		for i := range h.subs { // the same browser subscribing again: replace
			if h.subs[i].s.Endpoint == s.Endpoint {
				h.subs[i].s = s
				return gina.WaitMessage()
			}
		}
		h.next++
		h.subs = append(h.subs, sub{h.next, s})
		fmt.Printf("subscribed #%d (%d total)\n", h.next, len(h.subs))
	case tagUnsubscribe:
		h.drop(func(s sub) bool { return s.s.Endpoint == string(ctx.Data()) })
	case tagBroadcast:
		// One Send per subscriber; the payload is encrypted separately for each by the
		// delivery isolates. Results come back to this isolate as TagResult.
		payload := append([]byte(nil), ctx.Data()...)
		for _, s := range h.subs {
			wp.Send(ctx, &webpush.Notification{ID: s.id, Sub: s.s, Payload: payload, TTL: time.Hour, Topic: "gina-demo"})
		}
		fmt.Printf("pushing to %d subscriber(s)\n", len(h.subs))
	case webpush.TagResult:
		r := gina.PayloadAs[webpush.Result](m)
		fmt.Printf("  #%d: %v (HTTP %d, %d attempt(s))\n", r.ID, r.Outcome, r.Status, r.Attempts)
		if r.Outcome == webpush.OutcomeGone { // the user revoked it or it expired
			h.drop(func(s sub) bool { return s.id == r.ID })
		}
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// drop forgets the subscriptions that match.
func (h *hub) drop(match func(sub) bool) {
	kept := h.subs[:0]
	for _, s := range h.subs {
		if !match(s) {
			kept = append(kept, s)
		}
	}
	clear(h.subs[len(kept):])
	h.subs = kept
}

// ---- the page and the service worker ----

const page = `<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gina Web Push</title>
<body style="font:1rem system-ui;max-width:32rem;margin:3rem auto;padding:0 1rem">
<h1>Gina Web Push</h1>
<p><button id="on">Enable notifications</button> <button id="send">Send a notification</button></p>
<pre id="log" style="white-space:pre-wrap"></pre>
<script>
const log = s => document.getElementById('log').textContent += s + '\n';
const bytes = b64 => Uint8Array.from(atob(b64.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));

document.getElementById('on').onclick = async () => {
  if (!('serviceWorker' in navigator && 'PushManager' in window)) return log('Web Push is not available here (it needs https, or localhost).');
  const reg = await navigator.serviceWorker.register('/sw.js');
  await navigator.serviceWorker.ready;
  if (await Notification.requestPermission() !== 'granted') return log('Permission refused.');
  const key = (await (await fetch('/vapid-public-key')).text()).trim();
  const sub = await reg.pushManager.subscribe({userVisibleOnly: true, applicationServerKey: bytes(key)});
  const r = await fetch('/subscribe', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(sub)});
  log(r.ok ? 'Subscribed. Press "Send a notification".' : 'Server refused the subscription: ' + await r.text());
};
document.getElementById('send').onclick = async () => {
  const r = await fetch('/notify', {method: 'POST', body: 'Hello from Gina at ' + new Date().toLocaleTimeString()});
  log(await r.text());
};
</script>`

const serviceWorker = `
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(clients.claim()));
self.addEventListener('push', e => {
  let d = {title: 'Gina', body: ''};
  if (e.data) { try { d = e.data.json(); } catch { d.body = e.data.text(); } }
  e.waitUntil(self.registration.showNotification(d.title, {body: d.body}));
});
self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil(clients.openWindow('/'));
});
`

// loadVAPID reads the key pair from path, creating the file the first time.
func loadVAPID(path string) (*webpush.VAPID, error) {
	if b, err := os.ReadFile(path); err == nil {
		return webpush.ParseVAPID(strings.TrimSpace(string(b)))
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	v, err := webpush.GenerateVAPID()
	if err != nil {
		return nil, err
	}
	return v, os.WriteFile(path, []byte(v.PrivateKey()+"\n"), 0o600)
}

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	shards := flag.Int("shards", 2, "HTTP shards (the hub and the sender get one more shard of their own)")
	pin := flag.Bool("pin", false, "pin shard threads to CPUs")
	useTLS := flag.Bool("tls", false, "serve HTTPS with a throwaway self-signed certificate for localhost")
	keyFile := flag.String("vapid", "vapid.key", "file holding the VAPID private key (created when missing)")
	subject := flag.String("subject", "mailto:admin@example.com", "contact sent to push services (a mailto: or https: URL); use a real one")
	dev := flag.Bool("dev", false, "accept http and loopback push endpoints, to try the example against a fake push service")
	flag.Parse()

	vapid, err := loadVAPID(*keyFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "vapid:", err)
		os.Exit(1)
	}
	wp, err := webpush.New(webpush.Config{VAPID: vapid, Subject: *subject, Shard: *shards, AllowInsecure: *dev, AllowPrivate: *dev})
	if err != nil {
		fmt.Fprintln(os.Stderr, "webpush:", err)
		os.Exit(1)
	}
	hubH := gina.MakeHandle(uint8(*shards), typeHub, 0, 1) // the hub is the boot isolate of the last shard

	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.Bytes(200, "text/html; charset=utf-8", []byte(page)) })
	r.GET("/sw.js", func(c *ghttp.Context) {
		c.SetHeader("Cache-Control", "no-cache") // a service worker should be re-checked on every visit
		c.Bytes(200, "text/javascript; charset=utf-8", []byte(serviceWorker))
	})
	r.GET("/vapid-public-key", func(c *ghttp.Context) { c.String(200, vapid.PublicKey()) })
	r.POST("/subscribe", func(c *ghttp.Context) {
		if _, err := webpush.ParseSubscription(c.Req.Body); err != nil { // refuse bad ones here, where we can answer
			c.String(400, err.Error())
			return
		}
		c.Gina().SendRaw(hubH, tagSubscribe, c.Req.Body)
		c.String(201, "ok")
	})
	r.POST("/unsubscribe", func(c *ghttp.Context) {
		c.Gina().SendRaw(hubH, tagUnsubscribe, c.Req.Body)
		c.String(200, "ok")
	})
	r.POST("/notify", func(c *ghttp.Context) {
		msg := string(c.Req.Body)
		if len(msg) > 1000 {
			c.String(413, "message too long")
			return
		}
		payload, _ := json.Marshal(map[string]string{"title": "Gina", "body": msg})
		c.Gina().SendRaw(hubH, tagBroadcast, payload)
		c.String(200, "sent to every subscriber (see the server's output for the outcome)")
	})

	var tlsCfg *gtls.Config
	if *useTLS {
		cert, err := gtls.SelfSigned("localhost", "127.0.0.1")
		if err != nil {
			fmt.Fprintln(os.Stderr, "tls:", err)
			os.Exit(1)
		}
		tlsCfg = &gtls.Config{Certificates: []ctls.Certificate{cert}}
	}
	srv := ghttp.New(ghttp.Config{Port: uint16(*port), ReusePort: *shards > 1, TLS: tlsCfg}, r)

	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(typeHub, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 256}, nil, hubHandler(wp))},
		Shards: make([]gina.ShardSpec, *shards),
	}
	if err := srv.Install(&spec); err != nil { // http listener on shards 0..N-1 only
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}
	spec.Shards = append(spec.Shards, gina.ShardSpec{ // the shard of the hub and the sender: no listener
		Boot: []gina.SpawnSpec{{Type: typeHub, Group: gina.GroupRoot, Restart: gina.RestartPermanent}},
	})
	if err := wp.Install(&spec); err != nil {
		fmt.Fprintln(os.Stderr, "install:", err)
		os.Exit(1)
	}

	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v (listen: %v)\n", err, srv.ListenErr())
		os.Exit(1)
	}
	defer sys.Close()
	scheme := "http"
	if tlsCfg != nil {
		scheme = "https"
	}
	fmt.Printf("%d http shard(s) + hub/sender shard %d: %s://localhost:%d/\n", *shards, *shards, scheme, *port)
	sys.Run(gina.RunOptions{Pin: *pin})
}
