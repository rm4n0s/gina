//go:build !linux

package gina

// pinToCPU is only implemented on Linux.
func pinToCPU(int) {}
