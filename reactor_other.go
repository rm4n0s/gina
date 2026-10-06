//go:build !linux

package gina

import (
	"errors"
	"syscall"
)

// Non-Linux stub: the engine still builds and runs (no I/O); sockets are
// unavailable. The epoll reactor lives in reactor_linux.go.

var errNoIO = errors.New("gina: I/O reactor requires Linux")

type ioPoller struct{ epfd int }

func (sys *System) initIO() error  { return nil }
func (sys *System) closeIO()       {}
func (sys *System) pollIO(int) int { return 0 }

type reactor struct{}

func newReactor(*Shard, int, int, int) *reactor              { return &reactor{} }
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

var _ = syscall.ECANCELED
