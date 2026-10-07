// Package gina is a Go port of the Tina concurrency model: isolates (state
// machines) return effects, run on shards, and talk through fixed-size messages.
//
// Each shard owns its isolates, message pool, timers and sockets and runs on its
// own OS thread (System.Start), exchanging messages with other shards through
// lock-free SPSC rings; System.Step drives all shards cooperatively on one thread
// for tests and deterministic simulation. Goroutines and other concurrency
// primitives are confined to threads.go (and tests); the rest of the engine is
// single-threaded per shard.
package gina

import (
	"fmt"
	"net/netip"
	"reflect"
	"sync/atomic"
	"unsafe"
)

const (
	// MaxPayload is how much of a message's data fits in its fixed 128-byte
	// envelope. Longer data is allowed (see Blob and Ctx.SendRaw): it travels beside
	// the envelope, and the envelope's Payload then holds only its first MaxPayload
	// bytes. Typed payloads (Send, PayloadAs) are always inline and so limited to this.
	MaxPayload      = 96
	MaxInitArgs     = 64
	MaxSlotsPerType = 1<<20 - 1
	genMask         = 1<<28 - 1
)

// Handle is a generational reference to an isolate:
// shard:8 | type:8 | slot:20 | generation:28. Generations start at 1, so the
// zero Handle never refers to a live isolate.
type Handle uint64

func MakeHandle(shard uint8, typ TypeID, slot, gen uint32) Handle {
	return Handle(uint64(shard)<<56 | uint64(typ)<<48 | uint64(slot&0xFFFFF)<<28 | uint64(gen&genMask))
}
func (h Handle) Shard() uint8 { return uint8(h >> 56) }
func (h Handle) Type() TypeID { return TypeID(h >> 48) }
func (h Handle) Slot() uint32 { return uint32(h>>28) & 0xFFFFF }
func (h Handle) Gen() uint32  { return uint32(h) & genMask }

type TypeID uint8
type GroupID uint16

const (
	GroupRoot GroupID = 0
	GroupNone GroupID = 0xFFFF // unsupervised
)

type Tag uint16

// System tags are below TagUserBase.
const (
	TagShutdown  Tag = 1 // delivered to every isolate when shutdown begins
	TagYield     Tag = 2 // synthetic message for a turn after Yield with an empty mailbox
	TagChildExit Tag = 3 // payload ChildExit, sent to the spawning isolate
	TagUserBase  Tag = 0x40
)

// FlagLarge marks a message whose data is longer than MaxPayload. The data is an
// immutable buffer owned by the engine, read with Ctx.Data; Payload holds its
// first MaxPayload bytes and PayloadSize is MaxPayload.
const FlagLarge uint16 = 1

// Message is the fixed 128-byte envelope. Correlation is free for the sender to
// use (a request id, a stream id): it arrives unchanged.
type Message struct {
	Source      Handle
	Dest        Handle
	Correlation uint32
	Reserved    uint32
	Tag         Tag
	Flags       uint16
	PayloadSize uint16
	_           uint16
	Payload     [MaxPayload]byte
}

// IsLarge reports whether the message's data is longer than MaxPayload, in which
// case Payload holds only a prefix and Ctx.Data returns all of it.
func (m *Message) IsLarge() bool { return m.Flags&FlagLarge != 0 }

// Compile-time size assertions: both fail to compile unless Sizeof(Message) == 128.
const _ = uint(128 - unsafe.Sizeof(Message{}))
const _ = uint(unsafe.Sizeof(Message{}) - 128)

// ChildExit is the payload of TagChildExit. New is the zero Handle when the child
// was not restarted.
type ChildExit struct {
	Old, New Handle
	Kind     uint64 // an ExitKind
}

type SendResult uint8

const (
	SendOK SendResult = iota
	SendMailboxFull
	SendPoolExhausted
	SendStaleHandle
	SendRingFull
	SendAttachNotLocal
	SendPayloadTooLarge
)

func (r SendResult) String() string {
	return [...]string{"ok", "mailbox_full", "pool_exhausted", "stale_handle", "ring_full", "attach_not_local", "payload_too_large"}[r]
}

type SpawnError uint8

const (
	SpawnErrNone SpawnError = iota
	SpawnErrSlotsFull
	SpawnErrGroupFull
	SpawnErrGroupNotAllocated
	SpawnErrTypeNotAllocated
	SpawnErrInitFailed
	SpawnErrBadFD
)

func (e SpawnError) String() string {
	return [...]string{"ok", "slots_full", "group_full", "group_not_allocated", "type_not_allocated", "init_failed", "bad_fd"}[e]
}

type ExitKind uint8

const (
	ExitNormal ExitKind = iota
	ExitCrashed
	ExitShutdown
)

type RestartType uint8

const (
	RestartPermanent RestartType = iota // always restarted (unless shutting down)
	RestartTransient                    // restarted only after a crash
	RestartTemporary                    // never restarted
)

type FaultReason uint8

const (
	FaultNone FaultReason = iota
	FaultUser
	FaultPanic
	FaultInitFailed
	FaultContract
)

type EffectKind uint8

const (
	effDone EffectKind = iota + 1
	effYield
	effWaitMessage
	effCrash
)

// Effect is what a handler returns. It describes the isolate's next state; all
// actions (send, spawn, timers) happen through Ctx during the handler.
type Effect struct {
	Kind  EffectKind
	Fault FaultReason
}

func Done() Effect               { return Effect{Kind: effDone} }
func Yield() Effect              { return Effect{Kind: effYield} }
func WaitMessage() Effect        { return Effect{Kind: effWaitMessage} }
func Crash(r FaultReason) Effect { return Effect{Kind: effCrash, Fault: r} }

// SpawnSpec describes an isolate to start. Args is copied; use ArgsOf/ArgsAs.
type SpawnSpec struct {
	Type      TypeID
	Group     GroupID
	Restart   RestartType
	ArgsSize  uint8
	Args      [MaxInitArgs]byte
	HandoffFD FDHandle // socket the new isolate takes ownership of (closed when it dies)
}

// WithArgs returns a copy of s carrying a (pointer-free, <= 64 byte) argument value.
func WithArgs[A any](s SpawnSpec, a *A) SpawnSpec {
	checkPOD[A](MaxInitArgs)
	n := copy(s.Args[:], BytesOf(a))
	s.ArgsSize = uint8(n)
	return s
}

// ArgsAs copies init args into a value of type A.
func ArgsAs[A any](args []byte) A {
	var a A
	copy(BytesOf(&a), args)
	return a
}

// ---- payload helpers (the only place unsafe is used for messages) ----

// BytesOf views a pointer-free value as bytes.
func BytesOf[P any](p *P) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), unsafe.Sizeof(*p))
}

// PayloadAs views a message payload as *P. P must satisfy ValidatePayloadType.
func PayloadAs[P any](m *Message) *P {
	return (*P)(unsafe.Pointer(&m.Payload[0]))
}

// ValidatePayloadType reports whether P may travel in a message: pointer-free,
// padding-free (so no stale bytes leak), and at most MaxPayload bytes.
func ValidatePayloadType[P any]() error { return validatePOD(reflect.TypeFor[P](), MaxPayload) }

type podKey struct {
	t     reflect.Type
	limit uintptr
}

// podCache is copy-on-write so the hot path (Send[P] on many shard threads)
// reads it without locking. It is keyed by (type, limit): the same type can be
// valid as a 96-byte payload but not as 64-byte init args.
var podCache atomic.Pointer[map[podKey]error]

func checkPOD[P any](limit uintptr) {
	key := podKey{reflect.TypeFor[P](), limit}
	if m := podCache.Load(); m != nil {
		if err, ok := (*m)[key]; ok {
			if err != nil {
				panic(err)
			}
			return
		}
	}
	err := validatePOD(key.t, limit)
	for {
		old := podCache.Load()
		next := map[podKey]error{key: err}
		if old != nil {
			for k, v := range *old {
				next[k] = v
			}
		}
		if podCache.CompareAndSwap(old, &next) {
			break
		}
	}
	if err != nil {
		panic(err)
	}
}

func validatePOD(t reflect.Type, limit uintptr) error {
	if t.Size() > limit {
		return fmt.Errorf("gina: %s is %d bytes, limit %d", t, t.Size(), limit)
	}
	return podKind(t)
}

func podKind(t reflect.Type) error {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return nil
	case reflect.Array:
		return podKind(t.Elem())
	case reflect.Struct:
		var off uintptr
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Offset != off {
				return fmt.Errorf("gina: %s has implicit padding before field %s", t, f.Name)
			}
			if f.Name == "_" {
				return fmt.Errorf("gina: %s has a blank field", t)
			}
			if err := podKind(f.Type); err != nil {
				return err
			}
			off += f.Type.Size()
		}
		if off != t.Size() {
			return fmt.Errorf("gina: %s has trailing padding", t)
		}
		return nil
	}
	return fmt.Errorf("gina: %s (%s) is not allowed in a payload", t, t.Kind())
}

// I/O completion tags (system range). The payload is an IOResult.
const (
	TagIOAccept Tag = 4
	TagIORecv   Tag = 5
	TagIOSend   Tag = 6
)

const effWaitIO EffectKind = 5

// WaitIO parks the isolate until the I/O operation staged with ctx.IOAccept /
// IORecv / IOSend completes; the completion arrives as the next message.
func WaitIO() Effect { return Effect{Kind: effWaitIO} }

const effWaitAny EffectKind = 6

// WaitIOOrMessage is WaitIO for a read-side operation (IOAccept or IORecv) that
// a message may interrupt. The isolate parks until the operation completes or
// until any message is delivered to it. In the second case the operation is
// cancelled and its completion arrives first, as -ECANCELED, followed by the
// messages that were waiting; the isolate re-stages the read once it has dealt
// with them. A message that is already queued when the isolate parks interrupts
// the read at once, so mail is never left behind a parked read.
//
// A duplex connection needs this: it must read from its socket and still hear
// from other isolates. Writes are not interruptible (cancelling one would leave a
// partial send), so use WaitIO for IOSend. Returning WaitIOOrMessage without a
// staged IOAccept/IORecv is a contract violation and crashes the isolate; so is
// returning it from an init handler.
func WaitIOOrMessage() Effect { return Effect{Kind: effWaitAny} }

// IOResult is the payload of every I/O completion: a byte count (or, for accept,
// a new FDHandle) when >= 0, otherwise -errno.
type IOResult struct{ Result int64 }

// FDHandle is a generational reference to a socket in a shard's fd table:
// generation:32 | index+1:32. The zero value is never valid.
type FDHandle uint64

// TimerID identifies a pending timer for cancellation. The zero value is "none".
type TimerID uint64

type SubmitResult uint8

const (
	SubmitOK SubmitResult = iota
	SubmitAlreadyStaged
	SubmitBadFD
)

// ListenSpec describes a TCP listening socket (IPv4 by default). With ReusePort
// several listeners (one per shard, or one per process) can bind the same port
// and the kernel spreads incoming connections across them.
type ListenSpec struct {
	Addr [4]byte
	// IP, when valid, overrides Addr. An IPv4 address binds an IPv4 socket; an
	// IPv6 address binds an IPv6 socket, and "::" is dual-stack (it also accepts
	// IPv4 clients, reported as IPv4 addresses by PeerAddr).
	IP        netip.Addr
	Port      uint16 // 0 picks an ephemeral port; read it back with ctx.LocalPort
	ReusePort bool
	Backlog   int // default 1024
}
