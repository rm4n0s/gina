//go:build !linux

package gina

import (
	"errors"
	"syscall"
	"time"
)

// Non-Linux stub: the engine still builds and runs (no I/O); sockets are
// unavailable. The epoll reactor lives in reactor_linux.go.

var errNoIO = errors.New("gina: I/O reactor requires Linux")

type ioPoller struct{}

func (sys *System) initIO() error { return nil }
func (sys *System) closeIO()      {}
func (sys *System) waitIO(ms int) {
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

type reactor struct{}

func newReactor(*Shard, int, int) (*reactor, error)          { return &reactor{}, nil }
func (r *reactor) active() int                               { return 0 }
func (r *reactor) valid(FDHandle) bool                       { return false }
func (r *reactor) listen(ListenSpec) (FDHandle, error)       { return 0, errNoIO }
func (r *reactor) localPort(FDHandle) uint16                 { return 0 }
func (r *reactor) closeFD(FDHandle)                          {}
func (r *reactor) submit(Handle, *isoType, uint32, *ioStage) {}
func (r *reactor) cancelOp(int32)                            {}
func (r *reactor) finish(int32, int64)                       {}
func (r *reactor) timeoutOp(int32, uint32)                   {}
func (r *reactor) flush() bool                               { return false }
func (r *reactor) reset()                                    {}
func (r *reactor) close()                                    {}
func (r *reactor) wake()                                     {}
func (r *reactor) hasUndelivered() bool                      { return false }
func (r *reactor) poll(ms int) int {
	if ms > 0 { // no epoll: a bare sleep stands in for "wait for work"
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return 0
}

var _ = syscall.ECANCELED
