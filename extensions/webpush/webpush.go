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
//	wp.Install(&spec)                   // adds the sender isolate to shard 1
//	sys, _ := gina.NewSystem(spec, gina.Options{})
//	wp.Start(sys)                       // starts the workers that talk to push services
//	...
//	sub, _ := webpush.ParseSubscription(jsonFromTheBrowser)
//	wp.Send(ctx, &webpush.Notification{ID: 7, Sub: sub, Payload: []byte(`{"title":"Hi"}`)})
//
// # Isolates and threads
//
// Gina's TLS is a server only and its I/O reactor cannot connect out, so the
// request to the push service is made by a small pool of ordinary goroutines using
// net/http (transport.go, the only file here that starts goroutines, like
// threads.go in the engine). Everything else is plain isolate code. An isolate on
// any shard calls Send, which is a message to the sender isolate; the sender hands
// the notification to the pool without blocking; a worker encrypts, signs and
// posts it, and reports the outcome back with System.SendExternal as a TagResult
// message to the isolate that sent the notification (or Notification.ReplyTo).
// The shard that hosts the sender does almost nothing, so any shard will do, but a
// shard of its own keeps the crypto and the pool's wake-ups away from the
// connection shards.
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
// and GCM/FCM API keys (every current browser takes aes128gcm with VAPID), and
// batching or HTTP/2 connection management beyond what net/http does.
package webpush
