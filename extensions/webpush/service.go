package webpush

import (
	"errors"
	"net/http"
	"time"

	"github.com/rm4n0s/gina"
)

// Config configures a WebPush. Zero values take the documented defaults.
type Config struct {
	// VAPID identifies this server to push services. Required.
	VAPID *VAPID
	// Subject is a contact for the push service, "mailto:you@example.com" or an
	// https URL (RFC 8292 §2.1). Required: Apple's service rejects tokens without.
	Subject string

	// Shard is the shard that hosts the sender isolate (default 0). It must exist
	// when Install is called.
	Shard int
	// TypeID is the isolate type of the sender (default 220).
	TypeID gina.TypeID
	// Mailbox is the sender isolate's mailbox capacity (default 1024): Sends that
	// arrive between two of its turns wait there.
	Mailbox int

	// Workers is how many requests to push services can be in flight at once
	// (default 8). Queue is how many accepted notifications may wait for a worker
	// (default 1024); a full queue answers OutcomeOverloaded.
	Workers int
	Queue   int

	Timeout      time.Duration // per request (default 15s)
	Retries      int           // further attempts after a 429, 5xx or network error (default 2; negative: none)
	RetryBackoff time.Duration // wait before the first retry, doubling after (default 1s); Retry-After wins when longer
	DefaultTTL   time.Duration // Notification.TTL when zero (default 24h)

	// AllowInsecure accepts http:// endpoints and AllowPrivate lets the default
	// client connect to loopback, private and link-local addresses. Both are for
	// development and tests: otherwise a subscriber could point its "push service"
	// at your internal network (SSRF). Neither affects a Client you supply for the
	// address check, which is then yours to enforce.
	AllowInsecure bool
	AllowPrivate  bool

	// Client replaces the default HTTP client, e.g. to go through a proxy. It
	// should not follow redirects.
	Client *http.Client
}

func (c *Config) defaults() error {
	if c.VAPID == nil {
		return errors.New("webpush: Config.VAPID is required")
	}
	if c.Subject == "" {
		return errors.New("webpush: Config.Subject is required (a mailto: or https: contact)")
	}
	if c.TypeID == 0 {
		c.TypeID = 220
	}
	if c.Mailbox == 0 {
		c.Mailbox = 1024
	}
	if c.Workers == 0 {
		c.Workers = 8
	}
	if c.Queue == 0 {
		c.Queue = 1024
	}
	if c.Timeout == 0 {
		c.Timeout = 15 * time.Second
	}
	if c.Retries == 0 {
		c.Retries = 2
	} else if c.Retries < 0 {
		c.Retries = 0
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = time.Second
	}
	if c.DefaultTTL == 0 {
		c.DefaultTTL = 24 * time.Hour
	}
	if c.Shard < 0 || c.Workers < 0 || c.Queue < 0 || c.Mailbox < 0 {
		return errors.New("webpush: negative size in Config")
	}
	return nil
}

// WebPush is a Web Push sender: one isolate plus the workers behind it.
type WebPush struct {
	cfg Config
	t   *transport
}

// New makes a WebPush. Install it into a SystemSpec, then Start it once the
// System exists.
func New(cfg Config) (*WebPush, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	w := &WebPush{cfg: cfg}
	w.t = newTransport(w)
	w.t.setSender(gina.MakeHandle(uint8(cfg.Shard), cfg.TypeID, 0, 1)) // until the isolate starts and says
	return w, nil
}

// Install adds the sender's isolate type to spec and starts one instance on
// Config.Shard. Create the shards first.
func (w *WebPush) Install(spec *gina.SystemSpec) error {
	if w.cfg.Shard >= len(spec.Shards) {
		return errors.New("webpush: Config.Shard is not a shard of the spec")
	}
	spec.Types = append(spec.Types,
		gina.RegisterType(w.cfg.TypeID, gina.TypeOptions{SlotCount: 1, MailboxCapacity: w.cfg.Mailbox}, w.senderInit, w.senderHandler))
	spec.PoolSlots = max(spec.PoolSlots, 2*w.cfg.Mailbox, 4096)
	spec.Shards[w.cfg.Shard].Boot = append(spec.Shards[w.cfg.Shard].Boot,
		gina.SpawnSpec{Type: w.cfg.TypeID, Group: gina.GroupRoot, Restart: gina.RestartPermanent})
	return nil
}

// Sender is the handle of the sender isolate, the target of Send. It follows the
// isolate through restarts once the system has started.
func (w *WebPush) Sender() gina.Handle { return w.t.sender() }

// Start launches the workers. They report results through sys, so call it after
// gina.NewSystem and before or after sys.Run starts (Run blocks, so before it, in
// the usual case). Calling it twice is an error.
func (w *WebPush) Start(sys *gina.System) error { return w.t.start(sys) }

// Close stops the workers and abandons what they were doing. The sender isolate
// does the same when the system shuts down; Close is for a WebPush whose system
// never ran, or to stop sending while the system goes on. It is safe to call more
// than once.
func (w *WebPush) Close() { w.t.close() }
