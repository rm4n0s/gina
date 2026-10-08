package webpush

import (
	"crypto/x509"
	"errors"
	"net/netip"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rm4n0s/gina"
)

// The isolate types a WebPush registers, as offsets from Config.TypeID.
const (
	typeSenderOffset   = 0 // boot isolate: takes Send, queues, starts deliveries
	typeDeliveryOffset = 1 // one per notification in flight
	typeResolverOffset = 2 // boot isolate: DNS cache
	typeLookupOffset   = 3 // one per name being looked up
	typeCount          = 4
)

// Config configures a WebPush. Zero values take the documented defaults.
type Config struct {
	// VAPID identifies this server to push services. Required.
	VAPID *VAPID
	// Subject is a contact for the push service, "mailto:you@example.com" or an
	// https URL (RFC 8292 §2.1). Required: Apple's service rejects tokens without.
	Subject string

	// Shard is the shard that hosts the sender and everything it starts (default
	// 0). It must exist when Install is called.
	Shard int
	// TypeID is the first of four consecutive isolate type ids (default 220).
	TypeID gina.TypeID
	// Mailbox is the sender isolate's mailbox capacity (default 1024): Sends that
	// arrive between two of its turns wait there.
	Mailbox int

	// Workers is how many notifications can be in flight at once (default 8): each
	// is an isolate with a socket of its own. Queue is how many accepted
	// notifications may wait for a free one (default 1024); a full queue answers
	// OutcomeOverloaded. The shard needs about 2*Workers sockets (SystemSpec.MaxFDs)
	// and 4*Workers timers (TimerEntries); Install raises both if they are lower.
	Workers int
	Queue   int

	Timeout      time.Duration // per attempt, name lookup included (default 15s)
	Retries      int           // further attempts after a 429, 5xx or network error (default 2; negative: none)
	RetryBackoff time.Duration // wait before the first retry, doubling after (default 1s); Retry-After wins when longer
	DefaultTTL   time.Duration // Notification.TTL when zero (default 24h)

	// AllowInsecure accepts http:// endpoints and AllowPrivate lets the sender
	// connect to loopback, private and link-local addresses. Both are for
	// development and tests: otherwise a subscriber could point its "push service"
	// at your internal network (SSRF).
	AllowInsecure bool
	AllowPrivate  bool

	// RootCAs are the roots the push services' certificates must chain to (default
	// the system's, read once by New).
	RootCAs *x509.CertPool
	// Resolvers are the DNS servers to ask, in order (default /etc/resolv.conf,
	// else 127.0.0.1). Hosts maps names to addresses before any DNS query (default
	// /etc/hosts; pass an empty non-nil map for none). Names are lower case.
	// DNSTimeout is how long one server gets to answer (default 2s); every server
	// is tried twice.
	Resolvers  []netip.AddrPort
	Hosts      map[string][]netip.Addr
	DNSTimeout time.Duration
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
	if int(c.TypeID)+typeCount > 254 {
		return errors.New("webpush: Config.TypeID leaves no room for four isolate types")
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
	if c.DNSTimeout == 0 {
		c.DNSTimeout = 2 * time.Second
	}
	if c.Shard < 0 || c.Workers < 0 || c.Queue < 0 || c.Mailbox < 0 || c.DNSTimeout < 0 {
		return errors.New("webpush: negative size in Config")
	}
	if c.RootCAs == nil {
		pool, err := x509.SystemCertPool()
		if err != nil {
			if !c.AllowInsecure {
				return errors.New("webpush: no system root certificates (set Config.RootCAs): " + err.Error())
			}
			pool = x509.NewCertPool()
		}
		c.RootCAs = pool
	}
	if len(c.Resolvers) == 0 {
		c.Resolvers = systemResolvers()
	}
	if c.Hosts == nil {
		c.Hosts = systemHosts()
	} else {
		hosts := make(map[string][]netip.Addr, len(c.Hosts))
		for name, addrs := range c.Hosts {
			hosts[strings.ToLower(name)] = addrs
		}
		c.Hosts = hosts
	}
	return nil
}

// WebPush is a Web Push sender: a set of isolates on one shard.
type WebPush struct {
	cfg      Config
	sender   atomic.Uint64 // the sender's live handle: Send is called from every shard
	resolver atomic.Uint64
}

// New makes a WebPush. Install it into a SystemSpec.
func New(cfg Config) (*WebPush, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	w := &WebPush{cfg: cfg}
	// Until the isolates start and say, point at where they will be.
	w.sender.Store(uint64(gina.MakeHandle(uint8(cfg.Shard), cfg.TypeID+typeSenderOffset, 0, 1)))
	w.resolver.Store(uint64(gina.MakeHandle(uint8(cfg.Shard), cfg.TypeID+typeResolverOffset, 0, 1)))
	return w, nil
}

// Install adds the isolate types to spec and starts the sender and the resolver on
// Config.Shard. Create the shards first. Nothing else is needed: there are no
// threads or workers to start.
func (w *WebPush) Install(spec *gina.SystemSpec) error {
	if w.cfg.Shard >= len(spec.Shards) {
		return errors.New("webpush: Config.Shard is not a shard of the spec")
	}
	c := &w.cfg
	spec.Types = append(spec.Types,
		gina.RegisterType(c.TypeID+typeSenderOffset, gina.TypeOptions{SlotCount: 1, MailboxCapacity: c.Mailbox}, w.senderInit, w.senderHandler),
		gina.RegisterType(c.TypeID+typeDeliveryOffset, gina.TypeOptions{SlotCount: c.Workers, MailboxCapacity: 8}, nil, w.deliveryHandler),
		gina.RegisterType(c.TypeID+typeResolverOffset, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 4*c.Workers + 16}, w.resolverInit, w.resolverHandler),
		gina.RegisterType(c.TypeID+typeLookupOffset, gina.TypeOptions{SlotCount: c.Workers, MailboxCapacity: 8}, w.lookupInit, w.lookupHandler),
	)
	if spec.TimerEntries == 0 {
		spec.TimerEntries = 1024
	}
	if spec.MaxFDs == 0 {
		spec.MaxFDs = 4096
	}
	spec.PoolSlots = max(spec.PoolSlots, 2*c.Mailbox, 4096)
	spec.TimerEntries = max(spec.TimerEntries, 4*c.Workers+64)
	spec.MaxFDs = max(spec.MaxFDs, 2*c.Workers+16)
	spec.Shards[c.Shard].Boot = append(spec.Shards[c.Shard].Boot,
		gina.SpawnSpec{Type: c.TypeID + typeSenderOffset, Group: gina.GroupRoot, Restart: gina.RestartPermanent},
		gina.SpawnSpec{Type: c.TypeID + typeResolverOffset, Group: gina.GroupRoot, Restart: gina.RestartPermanent})
	return nil
}

// Sender is the handle of the sender isolate, the target of Send. It follows the
// isolate through restarts once the system has started.
func (w *WebPush) Sender() gina.Handle { return gina.Handle(w.sender.Load()) }

func (w *WebPush) resolverHandle() gina.Handle { return gina.Handle(w.resolver.Load()) }
