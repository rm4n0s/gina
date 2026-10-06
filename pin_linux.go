//go:build linux

package gina

import (
	"syscall"
	"unsafe"
)

// pinToCPU binds the calling thread to one CPU (sched_setaffinity).
func pinToCPU(cpu int) {
	var mask [16]uint64 // 1024 CPUs
	mask[cpu/64] |= 1 << (uint(cpu) % 64)
	syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, unsafe.Sizeof(mask), uintptr(unsafe.Pointer(&mask[0])))
}
