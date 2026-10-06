//go:build !linux

package gina

import "time"

type PreforkOptions struct {
	Pin        bool
	RestartMax int
	Window     time.Duration
}

func WorkerIndex() (int, int, bool) { return 0, 0, false }

// Prefork is only implemented on Linux.
func Prefork(int, PreforkOptions) int { panic("gina: Prefork requires Linux") }
