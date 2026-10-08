package gina

import (
	"errors"
	"net/netip"
	"time"
)

var errAlreadyOwns = errors.New("gina: isolate already owns a socket")

type ioStage struct {
	kind    uint8
	fd      FDHandle
	buf     []byte
	timeout time.Duration
}

const (
	ioAccept uint8 = iota + 1
	ioRecv
	ioSend
	ioConnect
)

func (c *Ctx) stage(kind uint8, fd FDHandle, buf []byte, timeout time.Duration) SubmitResult {
	if c.staged.kind != 0 {
		return SubmitAlreadyStaged
	}
	if !c.s.io.valid(fd) {
		return SubmitBadFD
	}
	c.staged = ioStage{kind: kind, fd: fd, buf: buf, timeout: timeout}
	return SubmitOK
}

// IOAccept stages an accept on a listening socket. Return WaitIO() to commit it;
// the completion (TagIOAccept) carries the new FDHandle or -errno. timeout 0 = none.
func (c *Ctx) IOAccept(fd FDHandle, timeout time.Duration) SubmitResult {
	return c.stage(ioAccept, fd, nil, timeout)
}

// IORecv stages a read into buf. The completion (TagIORecv) carries the byte
// count (0 = peer closed) or -errno (-ETIMEDOUT on timeout). buf is owned by the
// isolate and must stay untouched until the completion arrives.
func (c *Ctx) IORecv(fd FDHandle, buf []byte, timeout time.Duration) SubmitResult {
	return c.stage(ioRecv, fd, buf, timeout)
}

// IOSend stages a write of all of buf (the reactor resumes partial writes). The
// completion (TagIOSend) carries len(buf) or -errno.
func (c *Ctx) IOSend(fd FDHandle, buf []byte, timeout time.Duration) SubmitResult {
	return c.stage(ioSend, fd, buf, timeout)
}

// IOConnect stages the wait for a socket made by Dial to finish connecting. The
// completion (TagIOConnect) carries 0 on success or -errno (-ECONNREFUSED,
// -ETIMEDOUT, ...). Do not send or receive on a TCP socket before it arrives. It
// completes at once for a UDP socket or one that is already connected.
func (c *Ctx) IOConnect(fd FDHandle, timeout time.Duration) SubmitResult {
	return c.stage(ioConnect, fd, nil, timeout)
}

// Listen creates a listening socket owned by this isolate (closed when it dies).
func (c *Ctx) Listen(spec ListenSpec) (FDHandle, error) {
	if c.t.ownfd[c.slot] != 0 {
		return 0, errAlreadyOwns
	}
	fd, err := c.s.io.listen(spec)
	if err != nil {
		return 0, err
	}
	c.t.ownfd[c.slot] = fd
	return fd, nil
}

// Dial starts an outbound connection from this isolate and returns its socket,
// which the isolate owns (closed when it dies or calls CloseFD). A TCP connect is
// asynchronous: stage IOConnect and wait for it before using the socket. A UDP
// socket is connected at once, and IOSend and IORecv then carry one datagram per
// operation. Like Listen it takes the isolate's one socket slot.
//
// Dial does not decide where an isolate may connect: an application that dials
// addresses chosen by others must refuse the ones it does not want to reach.
func (c *Ctx) Dial(spec DialSpec) (FDHandle, error) {
	if c.t.ownfd[c.slot] != 0 {
		return 0, errAlreadyOwns
	}
	fd, err := c.s.io.dial(spec)
	if err != nil {
		return 0, err
	}
	c.t.ownfd[c.slot] = fd
	return fd, nil
}

// OwnedFD returns the socket handed to this isolate (SpawnSpec.HandoffFD or Listen).
func (c *Ctx) OwnedFD() FDHandle { return c.t.ownfd[c.slot] }

// CloseFD closes a socket; operations still pending on it complete with -ECANCELED.
func (c *Ctx) CloseFD(fd FDHandle) {
	if c.t.ownfd[c.slot] == fd {
		c.t.ownfd[c.slot] = 0
	}
	c.s.io.closeFD(fd)
}

// PeerAddr returns the remote address of a socket this shard accepted or dialled,
// recorded when it was made (no syscall). IPv4-mapped IPv6 addresses are reported as IPv4.
// ok is false for listeners, stale handles and sockets not made by accept.
func (c *Ctx) PeerAddr(fd FDHandle) (netip.AddrPort, bool) { return c.s.io.peerAddr(fd) }

// LocalPort returns the bound port of a socket (0 if unknown).
func (c *Ctx) LocalPort(fd FDHandle) uint16 { return c.s.io.localPort(fd) }
