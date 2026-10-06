//go:build linux

package gina

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

// PreforkOptions tunes Prefork.
type PreforkOptions struct {
	Pin        bool          // pin each worker's thread to one CPU (worker i -> CPU i % NumCPU)
	RestartMax int           // restarts allowed per RestartWindow before giving up (default 5)
	Window     time.Duration // default 10s
}

// WorkerIndex returns (index, count, true) inside a Prefork worker process.
func WorkerIndex() (int, int, bool) {
	w, err1 := strconv.Atoi(os.Getenv("GINA_WORKER"))
	n, err2 := strconv.Atoi(os.Getenv("GINA_WORKERS"))
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return w, n, true
}

// Prefork gives Gina multi-core scaling without threads, goroutines or shared
// memory: it re-executes the current binary n times and every copy runs its own
// shared-nothing System. Combined with ListenSpec.ReusePort each worker binds the
// same port and the kernel balances connections across them, which is the
// SO_REUSEPORT deployment Tina uses.
//
// Call it first thing in main. In a worker it returns the worker index (0..n-1).
// In the parent it never returns: the parent runs no isolates, only waits for
// workers (blocking wait4, no signals) and restarts a crashed worker, up to
// RestartMax times per Window, then exits non-zero. Workers are killed if the
// parent dies (PR_SET_PDEATHSIG). Workers do not share memory, so isolates in
// different workers cannot message each other; shards inside one worker can.
func Prefork(n int, o PreforkOptions) int {
	if w, count, ok := WorkerIndex(); ok {
		runtime.LockOSThread()
		if o.Pin {
			pinToCPU(w % runtime.NumCPU())
		}
		_ = count
		return w
	}
	if n < 1 {
		n = 1
	}
	if o.RestartMax == 0 {
		o.RestartMax = 5
	}
	if o.Window == 0 {
		o.Window = 10 * time.Second
	}
	runtime.LockOSThread() // PDEATHSIG is tied to the forking thread: keep it alive
	pids := make([]int, n)
	budgets := make([]budget, n)
	start := func(i int) error {
		attr := &syscall.ProcAttr{
			Env:   append(os.Environ(), "GINA_WORKER="+strconv.Itoa(i), "GINA_WORKERS="+strconv.Itoa(n)),
			Files: []uintptr{0, 1, 2},
			Sys:   &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL},
		}
		pid, err := syscall.ForkExec("/proc/self/exe", os.Args, attr)
		if err != nil {
			return err
		}
		pids[i] = pid
		return nil
	}
	for i := range pids {
		budgets[i] = newBudget(o.RestartMax, uint64(o.Window))
		if err := start(i); err != nil {
			fmt.Fprintf(os.Stderr, "gina: prefork: starting worker %d: %v\n", i, err)
			killAll(pids)
			os.Exit(1)
		}
	}
	t0 := time.Now()
	running := n
	for running > 0 {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, 0, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			break
		}
		for i := range pids {
			if pids[i] != pid {
				continue
			}
			pids[i] = 0
			if ws.Exited() && ws.ExitStatus() == 0 {
				running--
				break
			}
			fmt.Fprintf(os.Stderr, "gina: prefork: worker %d (pid %d) died (%v)\n", i, pid, ws)
			if budgets[i].exceeded(uint64(time.Since(t0))) {
				fmt.Fprintf(os.Stderr, "gina: prefork: worker %d restarting too often, giving up\n", i)
				killAll(pids)
				os.Exit(1)
			}
			if err := start(i); err != nil {
				fmt.Fprintf(os.Stderr, "gina: prefork: restarting worker %d: %v\n", i, err)
				killAll(pids)
				os.Exit(1)
			}
			break
		}
	}
	os.Exit(0)
	return 0
}

func killAll(pids []int) {
	for _, p := range pids {
		if p > 0 {
			syscall.Kill(p, syscall.SIGKILL)
		}
	}
}

func pinToCPU(cpu int) {
	var mask [16]uint64 // 1024 CPUs
	mask[cpu/64] |= 1 << (uint(cpu) % 64)
	syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, unsafe.Sizeof(mask), uintptr(unsafe.Pointer(&mask[0])))
}
