// Package webpush sends Web Push messages (RFC 8030) from a Gina system: the
// notifications a browser shows from its service worker even when the page is
// closed.
//
// The browser subscribes through its own push service (FCM for Chrome, Mozilla's
// autopush, Apple's) and hands the page a Subscription: an endpoint URL and two
// keys. The page sends that to the server, which stores it. To notify the user the
// server encrypts the message for the subscription (RFC 8291), signs a VAPID token
// that identifies it (RFC 8292) and POSTs both to the endpoint; the push service
// wakes the browser and the service worker shows the notification.
//
//	vapid, _ := webpush.GenerateVAPID() // once; keep vapid.PrivateKey() and serve vapid.PublicKey() to pages
//	wp, _ := webpush.New(webpush.Config{VAPID: vapid, Subject: "mailto:ops@example.com", Shard: 1})
//	wp.Install(&spec)                   // adds the sender and resolver isolates to shard 1
//	sys, _ := gina.NewSystem(spec, gina.Options{})
//	...
//	sub, _ := webpush.ParseSubscription(jsonFromTheBrowser)
//	wp.Send(ctx, &webpush.Notification{ID: 7, Sub: sub, Payload: []byte(`{"title":"Hi"}`)})
//
// # Isolates all the way down
//
// There are no goroutines, channels or locks here. Everything runs as isolates on
// the shard named by Config.Shard, over Gina's own sockets (Ctx.Dial) and its TLS
// 1.3 client (extensions/tls):
//
//   - sender (boot isolate): Send is a message to it. It validates the
//     notification, signs the VAPID token (cached per push service) and starts a
//     delivery, or queues the notification when Config.Workers are busy.
//   - delivery (one per notification in flight): encrypts the payload, asks the
//     resolver for the push service's addresses, connects, runs the TLS
//     handshake, POSTs over HTTP/1.1 and reads the status and headers. It retries
//     where that is worth it, reports a TagResult to the isolate that sent the
//     notification (or Notification.ReplyTo) and exits.
//   - resolver (boot isolate): a DNS cache that merges concurrent requests for a
//     name and starts a lookup for each name it does not know.
//   - lookup (one per name being resolved): asks the nameservers over UDP for A and
//     AAAA records (Config.Resolvers, by default those of /etc/resolv.conf) and
//     reports to the resolver.
//
// An isolate on any shard calls Send. The shard that hosts the sender is busy
// only with crypto and sockets, so a shard of its own keeps that away from the
// connection shards.
//
// Each notification gets its own connection (Connection: close), so a burst to one
// push service pays one TLS handshake per notification; there is no connection
// pool and no HTTP/2. DNS is plain UDP without DNSSEC; a truncated answer is used
// only if it carries addresses.
//
// Delivery is best effort and at most once per Send: a full queue, mailbox or ring
// drops the notification (a full queue says so in a Result), and a notification
// not yet sent when the system stops is discarded.
//
// # What a Result means
//
// OutcomeDelivered means the push service accepted the message (HTTP 201), not
// that the browser showed it. OutcomeGone (404 or 410) means the subscription no
// longer exists and should be deleted. See Outcome for the rest.
//
// # Not implemented
//
// Push receipts (the Prefer: respond-async flow), the old aesgcm content coding
// and GCM/FCM API keys (every current browser takes aes128gcm with VAPID),
// connection reuse and HTTP/2, DNS over TCP, and TLS 1.2 (push services speak
// TLS 1.3).
package webpush
