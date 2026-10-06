//go:build linux

package gina

import (
	"syscall"
	"time"
)

const (
	epollET     = uint32(1) << 31
	soReusePort = 15
)

// ioPoller is used only by the single-thread driver (System.Step/RunUntilIdle):
// a master epoll instance that watches every shard's own epoll fd, so one
// blocking wait can serve all shards. Threaded shards (System.Start) block in
// their own epoll instead.
type ioPoller struct {
	master int
	events [8]syscall.EpollEvent
}

func (sys *System) initIO() error {
	ep, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return err
	}
	sys.iop.master = ep
	for _, sh := range sys.shards {
		ev := syscall.EpollEvent{Events: uint32(syscall.EPOLLIN)}
		if err := syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, sh.io.epfd, &ev); err != nil {
			syscall.Close(ep)
			return err
		}
	}
	return nil
}

func (sys *System) closeIO() {
	if sys.iop.master > 0 {
		syscall.Close(sys.iop.master)
		sys.iop.master = 0
	}
}

// waitIO blocks up to timeoutMs (-1 = forever) until some shard's epoll has
// events; the caller then ticks the shards, which poll for them.
func (sys *System) waitIO(timeoutMs int) {
	syscall.EpollWait(sys.iop.master, sys.iop.events[:], timeoutMs)
}

type fdEntry struct {
	raw      int32
	gen      uint32
	used     bool
	rop, wop int32 // pending read-side (accept/recv) and write-side (send) op, or -1
}

type ioOp struct {
	kind   uint8
	active bool
	done   bool // result computed but not yet delivered (pool was exhausted)
	owner  Handle
	fd     FDHandle
	buf    []byte
	off    int
	seq    uint32
	timer  TimerID
	result int64
}

// reactor is a shard's readiness-based I/O engine presented with completion
// semantics: an operation is attempted immediately, and only if the socket
// would block is it parked until epoll reports the socket (edge-triggered).
type reactor struct {
	s       *Shard
	epfd    int // this shard's own epoll instance
	wakefd  int // eventfd other shards write to wake this shard; registered in epfd with Fd=-1
	events  [64]syscall.EpollEvent
	fds     []fdEntry
	freeFDs []int32
	ops     []ioOp
	freeOps []int32
	opSeq   uint32
	nActive int
	nUndel  int
}

func newReactor(s *Shard, maxFDs, maxOps int) (*reactor, error) {
	ep, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	efd, _, e := syscall.RawSyscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != 0 {
		syscall.Close(ep)
		return nil, e
	}
	ev := syscall.EpollEvent{Events: uint32(syscall.EPOLLIN), Fd: -1}
	if err := syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, int(efd), &ev); err != nil {
		syscall.Close(int(efd))
		syscall.Close(ep)
		return nil, err
	}
	r := &reactor{s: s, epfd: ep, wakefd: int(efd), fds: make([]fdEntry, maxFDs), ops: make([]ioOp, maxOps)}
	for i := range r.fds {
		r.fds[i] = fdEntry{raw: -1, gen: 1, rop: -1, wop: -1}
	}
	r.refill()
	return r, nil
}

// poll waits up to timeoutMs (0 = just check, -1 = forever) and completes the
// operations of every ready socket. It returns how many sockets it handled.
func (r *reactor) poll(timeoutMs int) int {
	n, err := syscall.EpollWait(r.epfd, r.events[:], timeoutMs)
	if err != nil {
		return 0 // EINTR
	}
	handled := 0
	for i := 0; i < n; i++ {
		ev := &r.events[i]
		if ev.Fd < 0 { // another shard (or Stop) woke us: reset the eventfd
			var b [8]byte
			syscall.Read(r.wakefd, b[:])
			continue
		}
		r.onEvent(int(ev.Fd))
		handled++
	}
	return handled
}

// wake makes a blocked poll return. It is safe to call from any thread.
func (r *reactor) wake() {
	one := [8]byte{1}
	syscall.Write(r.wakefd, one[:])
}

func (r *reactor) hasUndelivered() bool { return r.nUndel > 0 }

// close releases the epoll instance and the eventfd (reset closes the sockets).
func (r *reactor) close() {
	syscall.Close(r.wakefd)
	syscall.Close(r.epfd)
}

func (r *reactor) refill() {
	r.freeFDs = r.freeFDs[:0]
	for i := len(r.fds) - 1; i >= 0; i-- {
		r.freeFDs = append(r.freeFDs, int32(i))
	}
	r.freeOps = r.freeOps[:0]
	for i := len(r.ops) - 1; i >= 0; i-- {
		r.freeOps = append(r.freeOps, int32(i))
	}
}

func (r *reactor) active() int { return r.nActive }

func (r *reactor) fdIndex(h FDHandle) int {
	idx := int(uint32(h)) - 1
	if idx < 0 || idx >= len(r.fds) {
		return -1
	}
	if e := &r.fds[idx]; !e.used || e.gen != uint32(h>>32) {
		return -1
	}
	return idx
}

func (r *reactor) valid(h FDHandle) bool { return r.fdIndex(h) >= 0 }

func (r *reactor) register(raw int, listener bool) (FDHandle, bool) {
	n := len(r.freeFDs)
	if n == 0 {
		return 0, false
	}
	idx := r.freeFDs[n-1]
	events := uint32(syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP) | epollET
	if listener {
		events = uint32(syscall.EPOLLIN) | epollET
	}
	ev := syscall.EpollEvent{Events: events, Fd: idx, Pad: int32(r.s.id)}
	if err := syscall.EpollCtl(r.epfd, syscall.EPOLL_CTL_ADD, raw, &ev); err != nil {
		return 0, false
	}
	r.freeFDs = r.freeFDs[:n-1]
	e := &r.fds[idx]
	e.raw, e.used, e.rop, e.wop = int32(raw), true, -1, -1
	return FDHandle(uint64(e.gen)<<32 | uint64(idx+1)), true
}

func (r *reactor) listen(spec ListenSpec) (FDHandle, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (FDHandle, error) { syscall.Close(fd); return 0, err }
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return fail(err)
	}
	if spec.ReusePort {
		if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1); err != nil {
			return fail(err)
		}
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: int(spec.Port), Addr: spec.Addr}); err != nil {
		return fail(err)
	}
	backlog := spec.Backlog
	if backlog == 0 {
		backlog = 1024
	}
	if err := syscall.Listen(fd, backlog); err != nil {
		return fail(err)
	}
	h, ok := r.register(fd, true)
	if !ok {
		return fail(syscall.EMFILE)
	}
	return h, nil
}

func (r *reactor) localPort(h FDHandle) uint16 {
	idx := r.fdIndex(h)
	if idx < 0 {
		return 0
	}
	sa, err := syscall.Getsockname(int(r.fds[idx].raw))
	if err != nil {
		return 0
	}
	if in4, ok := sa.(*syscall.SockaddrInet4); ok {
		return uint16(in4.Port)
	}
	return 0
}

func (r *reactor) closeFD(h FDHandle) {
	idx := r.fdIndex(h)
	if idx < 0 {
		return
	}
	e := &r.fds[idx]
	if e.rop >= 0 {
		r.finish(e.rop, -int64(syscall.ECANCELED))
	}
	if e.wop >= 0 {
		r.finish(e.wop, -int64(syscall.ECANCELED))
	}
	syscall.Close(int(e.raw)) // also removes it from the epoll set
	e.used, e.raw = false, -1
	if e.gen++; e.gen == 0 {
		e.gen = 1
	}
	r.freeFDs = append(r.freeFDs, int32(idx))
}

// deliverDirect completes an operation that never got an op slot.
func (r *reactor) deliverDirect(owner Handle, kind uint8, res int64) {
	var m Message
	m.Tag = ioTag(kind)
	m.PayloadSize = uint16(copy(m.Payload[:], BytesOf(&IOResult{Result: res})))
	r.s.enqueueFront(owner, &m)
}

func ioTag(kind uint8) Tag {
	switch kind {
	case ioAccept:
		return TagIOAccept
	case ioRecv:
		return TagIORecv
	}
	return TagIOSend
}

func (r *reactor) submit(owner Handle, t *isoType, slot uint32, st *ioStage) {
	idx := r.fdIndex(st.fd)
	if idx < 0 {
		r.deliverDirect(owner, st.kind, -int64(syscall.EBADF))
		return
	}
	e := &r.fds[idx]
	if (st.kind == ioSend && e.wop >= 0) || (st.kind != ioSend && e.rop >= 0) {
		r.deliverDirect(owner, st.kind, -int64(syscall.EBUSY))
		return
	}
	n := len(r.freeOps)
	if n == 0 {
		r.deliverDirect(owner, st.kind, -int64(syscall.ENOMEM))
		return
	}
	oi := r.freeOps[n-1]
	r.freeOps = r.freeOps[:n-1]
	r.opSeq++
	op := &r.ops[oi]
	*op = ioOp{kind: st.kind, active: true, owner: owner, fd: st.fd, buf: st.buf, seq: r.opSeq}
	t.ioop[slot] = oi
	r.nActive++
	if st.timeout > 0 {
		op.timer = r.s.timers.push(timerEntry{due: r.s.sys.clock.Now() + uint64(st.timeout/time.Nanosecond), op: oi, opSeq: op.seq})
	}
	if res, done := r.try(oi, idx); done {
		r.finish(oi, res)
		return
	}
	if st.kind == ioSend {
		e.wop = oi
	} else {
		e.rop = oi
	}
}

// try attempts the operation's syscall; done=false means the socket would block.
func (r *reactor) try(oi int32, idx int) (res int64, done bool) {
	op := &r.ops[oi]
	raw := int(r.fds[idx].raw)
	switch op.kind {
	case ioAccept:
		for {
			nfd, _, err := syscall.Accept4(raw, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
			switch err {
			case nil:
				syscall.SetsockoptInt(nfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
				h, ok := r.register(nfd, false)
				if !ok {
					syscall.Close(nfd)
					return -int64(syscall.EMFILE), true
				}
				return int64(h), true
			case syscall.EAGAIN:
				return 0, false
			case syscall.EINTR, syscall.ECONNABORTED:
				continue
			default:
				return -int64(errno(err)), true
			}
		}
	case ioRecv:
		for {
			n, err := syscall.Read(raw, op.buf)
			switch err {
			case nil:
				return int64(n), true
			case syscall.EAGAIN:
				return 0, false
			case syscall.EINTR:
				continue
			default:
				return -int64(errno(err)), true
			}
		}
	default: // ioSend
		for op.off < len(op.buf) {
			n, err := syscall.Write(raw, op.buf[op.off:])
			switch err {
			case nil:
				op.off += n
			case syscall.EAGAIN:
				return 0, false
			case syscall.EINTR:
			default:
				return -int64(errno(err)), true
			}
		}
		return int64(len(op.buf)), true
	}
}

func errno(err error) syscall.Errno {
	if e, ok := err.(syscall.Errno); ok {
		return e
	}
	return syscall.EIO
}

// onEvent retries the operations parked on a socket epoll just reported.
func (r *reactor) onEvent(idx int) {
	if idx < 0 || idx >= len(r.fds) || !r.fds[idx].used {
		return
	}
	if oi := r.fds[idx].rop; oi >= 0 {
		if res, done := r.try(oi, idx); done {
			r.finish(oi, res)
		}
	}
	if oi := r.fds[idx].wop; oi >= 0 {
		if res, done := r.try(oi, idx); done {
			r.finish(oi, res)
		}
	}
}

// finish completes an operation and wakes its isolate with the result.
func (r *reactor) finish(oi int32, res int64) {
	op := &r.ops[oi]
	if !op.active || op.done {
		return
	}
	if idx := r.fdIndex(op.fd); idx >= 0 {
		if e := &r.fds[idx]; e.rop == oi {
			e.rop = -1
		} else if e.wop == oi {
			e.wop = -1
		}
	}
	if op.timer != 0 {
		r.s.timers.cancel(op.timer)
		op.timer = 0
	}
	if !r.deliverOp(op, res) {
		op.done, op.result = true, res // pool exhausted: retry from flush
		r.nUndel++
		return
	}
	r.release(oi)
}

func (r *reactor) deliverOp(op *ioOp, res int64) bool {
	var m Message
	m.Tag = ioTag(op.kind)
	m.PayloadSize = uint16(copy(m.Payload[:], BytesOf(&IOResult{Result: res})))
	switch r.s.enqueueFront(op.owner, &m) {
	case SendPoolExhausted, SendMailboxFull:
		return false
	case SendStaleHandle:
		if op.kind == ioAccept && res >= 0 {
			r.closeFD(FDHandle(res)) // nobody to hand the new socket to
		}
	}
	return true
}

func (r *reactor) release(oi int32) {
	op := &r.ops[oi]
	if t := r.s.types[op.owner.Type()]; t != nil {
		if slot := op.owner.Slot(); int(slot) < t.slots && t.gen[slot] == op.owner.Gen() && t.ioop[slot] == oi {
			t.ioop[slot] = -1
		}
	}
	op.active, op.done, op.buf = false, false, nil
	r.freeOps = append(r.freeOps, oi)
	r.nActive--
}

// flush retries completions that could not be delivered earlier.
func (r *reactor) flush() bool {
	if r.nUndel == 0 {
		return false
	}
	any := false
	for i := range r.ops {
		if op := &r.ops[i]; op.active && op.done && r.deliverOp(op, op.result) {
			r.nUndel--
			r.release(int32(i))
			any = true
		}
	}
	return any
}

// timeoutOp is called when an operation's timer fires.
func (r *reactor) timeoutOp(oi int32, seq uint32) {
	op := &r.ops[oi]
	if !op.active || op.seq != seq || op.done {
		return
	}
	op.timer = 0
	r.finish(oi, -int64(syscall.ETIMEDOUT))
}

// cancelOp drops an operation whose owner died; nothing is delivered.
func (r *reactor) cancelOp(oi int32) {
	op := &r.ops[oi]
	if !op.active {
		return
	}
	if idx := r.fdIndex(op.fd); idx >= 0 {
		if e := &r.fds[idx]; e.rop == oi {
			e.rop = -1
		} else if e.wop == oi {
			e.wop = -1
		}
	}
	if op.timer != 0 {
		r.s.timers.cancel(op.timer)
	}
	if op.done {
		r.nUndel--
	}
	op.active, op.done, op.buf = false, false, nil
	r.freeOps = append(r.freeOps, oi)
	r.nActive--
}

// reset closes every socket and drops every operation (Level-2 reset, shutdown).
func (r *reactor) reset() {
	for i := range r.fds {
		e := &r.fds[i]
		if e.used {
			syscall.Close(int(e.raw))
			if e.gen++; e.gen == 0 {
				e.gen = 1
			}
		}
		e.used, e.raw, e.rop, e.wop = false, -1, -1, -1
	}
	for i := range r.ops {
		r.ops[i] = ioOp{}
	}
	r.nActive, r.nUndel = 0, 0
	r.refill()
}
