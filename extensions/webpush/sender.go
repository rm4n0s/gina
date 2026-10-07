package webpush

import (
	"encoding/binary"
	"errors"
	"net/url"
	"time"

	"github.com/rm4n0s/gina"
)

const (
	// TagPush is delivered to the sender isolate; use Send rather than building it.
	TagPush gina.Tag = gina.TagUserBase + 0x20
	// TagResult is delivered to the isolate that sent a notification (or to its
	// ReplyTo) when the push service has answered or the attempt was given up. The
	// payload is a Result (gina.PayloadAs[webpush.Result]).
	TagResult gina.Tag = gina.TagUserBase + 0x21
)

// Urgency is the Urgency header of RFC 8030 §5.3: how soon the push service should
// wake a device that is asleep. The zero value sends no header (normal).
type Urgency uint8

const (
	UrgencyDefault Urgency = iota
	UrgencyVeryLow
	UrgencyLow
	UrgencyNormal
	UrgencyHigh
)

var urgencyNames = [...]string{"", "very-low", "low", "normal", "high"}

// Notification is one message for one subscription.
type Notification struct {
	// ID is returned unchanged in the Result: use it to find the subscription
	// again (a database key, an index) when the push service says it is gone.
	ID  uint64
	Sub Subscription
	// Payload is delivered to the service worker's push event (event.data), up to
	// MaxPayload bytes. Empty sends a "tickle" with no data.
	Payload []byte
	// TTL is how long the push service may hold the message while the device is
	// offline. Zero takes Config.DefaultTTL; a negative value means deliver now or
	// never.
	TTL     time.Duration
	Urgency Urgency
	// Topic, if set, lets a newer message replace an undelivered older one with the
	// same topic (RFC 8030 §5.4): up to 32 characters of A-Z a-z 0-9 _ -.
	Topic string
	// Pad hides the message's length: that many zero bytes are added inside the
	// encrypted record (Payload plus Pad is at most MaxPayload).
	Pad int
	// ReplyTo receives the Result. The zero Handle means the isolate that called
	// Send, which is gone by the time a short-lived one (an HTTP connection)
	// could read it: give those a long-lived isolate here.
	ReplyTo gina.Handle
}

// Outcome says how a notification ended.
type Outcome uint8

const (
	// OutcomeDelivered: the push service accepted the message (2xx).
	OutcomeDelivered Outcome = iota + 1
	// OutcomeGone: 404 or 410, the subscription expired or the user revoked it.
	// Delete it.
	OutcomeGone
	// OutcomeRejected: the push service refused the message and retrying will not
	// help (400, 401, 403, 413, ...). Status says which; 401 or 403 usually means
	// the VAPID key or Subject is not acceptable to that service.
	OutcomeRejected
	// OutcomeFailed: the push service could not be reached, or kept answering 429
	// or 5xx, after Config.Retries retries. Status is the last HTTP status or 0.
	OutcomeFailed
	// OutcomeInvalid: the notification itself is unusable (bad keys, payload too
	// large, endpoint not allowed). Nothing was sent.
	OutcomeInvalid
	// OutcomeOverloaded: the sender's queue was full, or the sender is not running.
	// Nothing was sent.
	OutcomeOverloaded
)

func (o Outcome) String() string {
	switch o {
	case OutcomeDelivered:
		return "delivered"
	case OutcomeGone:
		return "gone"
	case OutcomeRejected:
		return "rejected"
	case OutcomeFailed:
		return "failed"
	case OutcomeInvalid:
		return "invalid"
	case OutcomeOverloaded:
		return "overloaded"
	}
	return "unknown"
}

// Result is the payload of TagResult.
type Result struct {
	ID       uint64 // Notification.ID
	Status   int32  // HTTP status of the last attempt, 0 if there was none
	Attempts uint8  // requests made
	Outcome  Outcome
	_pad     [2]byte
}

// Validate reports a problem that would make the sender refuse the notification.
// The sender checks again, so calling it is only for failing early.
func (n *Notification) Validate() error {
	if err := n.Sub.Validate(); err != nil {
		return err
	}
	return n.validateFields()
}

func (n *Notification) validateFields() error {
	if len(n.Payload) > MaxPayload || n.Pad < 0 || len(n.Payload)+n.Pad > MaxPayload {
		return errors.New("webpush: payload too large")
	}
	if int(n.Urgency) >= len(urgencyNames) {
		return errors.New("webpush: bad urgency")
	}
	if len(n.Topic) > 32 {
		return errors.New("webpush: topic is longer than 32 characters")
	}
	for i := 0; i < len(n.Topic); i++ {
		c := n.Topic[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return errors.New("webpush: topic must use only A-Z a-z 0-9 _ -")
		}
	}
	return nil
}

// ---- wire format: a Notification as the data of a TagPush message ----

const wireVersion = 1

func appendLP(b []byte, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func (n *Notification) marshal() []byte {
	b := make([]byte, 0, 40+len(n.Sub.Endpoint)+len(n.Sub.P256dh)+len(n.Sub.Auth)+len(n.Topic)+len(n.Payload)+4*binary.MaxVarintLen32)
	b = append(b, wireVersion)
	b = binary.BigEndian.AppendUint64(b, n.ID)
	b = binary.BigEndian.AppendUint64(b, uint64(n.ReplyTo))
	ttl := int64(-1) // -1: take the default
	switch {
	case n.TTL < 0:
		ttl = 0
	case n.TTL > 0:
		ttl = int64(n.TTL / time.Second)
	}
	b = binary.BigEndian.AppendUint64(b, uint64(ttl))
	b = append(b, byte(n.Urgency))
	b = binary.BigEndian.AppendUint32(b, uint32(max(n.Pad, 0)))
	b = appendLP(b, []byte(n.Sub.Endpoint))
	b = appendLP(b, n.Sub.P256dh)
	b = appendLP(b, n.Sub.Auth)
	b = appendLP(b, []byte(n.Topic))
	return append(b, n.Payload...) // the rest
}

var errWire = errors.New("webpush: malformed notification")

// unmarshal copies everything out of b, which is only valid during the turn.
func unmarshal(b []byte) (n Notification, ttlSec int64, err error) {
	if len(b) < 1+8+8+8+1+4 || b[0] != wireVersion {
		return n, 0, errWire
	}
	n.ID = binary.BigEndian.Uint64(b[1:])
	n.ReplyTo = gina.Handle(binary.BigEndian.Uint64(b[9:]))
	ttlSec = int64(binary.BigEndian.Uint64(b[17:]))
	n.Urgency = Urgency(b[25])
	n.Pad = int(binary.BigEndian.Uint32(b[26:]))
	b = b[30:]
	lp := func() []byte {
		l, k := binary.Uvarint(b)
		if k <= 0 || l > uint64(len(b)-k) {
			err = errWire
			return nil
		}
		v := b[k : k+int(l)]
		b = b[k+int(l):]
		return v
	}
	n.Sub.Endpoint = string(lp())
	n.Sub.P256dh = append([]byte(nil), lp()...)
	n.Sub.Auth = append([]byte(nil), lp()...)
	n.Topic = string(lp())
	if err != nil {
		return n, 0, err
	}
	n.Payload = append([]byte(nil), b...)
	return n, ttlSec, nil
}

// Send asks the sender isolate to deliver n. It can be called from any isolate on
// any shard. The result says only whether Gina accepted the message; the outcome of
// the delivery arrives later as a TagResult.
func Send(g *gina.Ctx, sender gina.Handle, n *Notification) gina.SendResult {
	return g.SendRaw(sender, TagPush, n.marshal())
}

// Send is webpush.Send to this WebPush's sender.
func (w *WebPush) Send(g *gina.Ctx, n *Notification) gina.SendResult {
	return Send(g, w.Sender(), n)
}

// SendExternal is Send for code that is not an isolate (a goroutine, a signal
// handler, a test). It needs the system to be built: call it after Install and
// NewSystem.
func (w *WebPush) SendExternal(sys *gina.System, n *Notification) gina.SendResult {
	return sys.SendExternal(w.Sender(), TagPush, n.marshal())
}

// ---- the sender isolate ----

type sender struct{ accepted uint64 }

func (w *WebPush) senderInit(_ *sender, g *gina.Ctx, _ []byte) gina.Effect {
	w.t.setSender(g.Self()) // after a restart the handle has a new generation
	return gina.WaitMessage()
}

func (w *WebPush) senderHandler(s *sender, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case TagPush:
		n, ttl, err := unmarshal(g.Data())
		if err == nil {
			err = n.validateFields()
		}
		if err == nil {
			err = w.checkEndpoint(n.Sub.Endpoint)
		}
		reply := n.ReplyTo
		if reply == 0 {
			reply = m.Source
		}
		switch {
		case err != nil:
			w.reply(g, reply, Result{ID: n.ID, Outcome: OutcomeInvalid})
		case !w.t.submit(&job{n: n, ttl: w.ttl(ttl), reply: reply}):
			w.reply(g, reply, Result{ID: n.ID, Outcome: OutcomeOverloaded})
		default:
			s.accepted++
		}
	case gina.TagShutdown:
		w.t.close()
		return gina.Done()
	}
	return gina.WaitMessage()
}

func (w *WebPush) reply(g *gina.Ctx, to gina.Handle, r Result) {
	if to != 0 {
		g.SendRaw(to, TagResult, gina.BytesOf(&r))
	}
}

// ttl turns the wire value (-1 default, else seconds) into the header value.
func (w *WebPush) ttl(sec int64) int64 {
	if sec < 0 {
		return int64(w.cfg.DefaultTTL / time.Second)
	}
	return sec
}

// checkEndpoint is the policy on where a subscription may point. The endpoint is
// chosen by whoever sent the subscription, so the server must not follow it to
// just anywhere: https only, unless Config.AllowInsecure.
func (w *WebPush) checkEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil {
		return errors.New("webpush: bad endpoint")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && w.cfg.AllowInsecure) {
		return errors.New("webpush: endpoint must be https")
	}
	return nil
}
