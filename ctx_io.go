package gina

import (
	"errors"
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

// OwnedFD returns the socket handed to this isolate (SpawnSpec.HandoffFD or Listen).
func (c *Ctx) OwnedFD() FDHandle { return c.t.ownfd[c.slot] }

// CloseFD closes a socket; operations still pending on it complete with -ECANCELED.
func (c *Ctx) CloseFD(fd FDHandle) {
	if c.t.ownfd[c.slot] == fd {
		c.t.ownfd[c.slot] = 0
	}
	c.s.io.closeFD(fd)
}

// LocalPort returns the bound port of a socket (0 if unknown).
func (c *Ctx) LocalPort(fd FDHandle) uint16 { return c.s.io.localPort(fd) }
