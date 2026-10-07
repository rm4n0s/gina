//go:build linux

package gina_test

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"gina"
)

const (
	tagIntrStart = gina.TagUserBase + 0x40 + iota
	tagIntrMail
	tagIntrSelfMail
	tagIntrBare
)

// intrIso listens and parks in WaitIOOrMessage; log records what it saw.
type intrIso struct {
	fd  gina.FDHandle
	log *[]string
}

func intrHandler(log *[]string) gina.Handler[intrIso] {
	return func(self *intrIso, g *gina.Ctx, m *gina.Message) gina.Effect {
		rec := func(s string) { *log = append(*log, s) }
		park := func() gina.Effect {
			g.IOAccept(self.fd, 0)
			return gina.WaitIOOrMessage()
		}
		switch m.Tag {
		case tagIntrStart:
			fd, err := g.Listen(gina.ListenSpec{Addr: [4]byte{127, 0, 0, 1}, Port: 0})
			if err != nil {
				panic(err)
			}
			self.fd = fd
			rec(fmt.Sprintf("port=%d", g.LocalPort(fd)))
			return park()
		case gina.TagIOAccept:
			res := gina.PayloadAs[gina.IOResult](m).Result
			if res == -int64(syscall.ECANCELED) {
				rec("interrupted")
				return gina.Yield() // let the mail through
			}
			rec("accepted")
			g.CloseFD(gina.FDHandle(res))
			return park()
		case tagIntrMail:
			rec("mail")
			return gina.Yield()
		case tagIntrSelfMail: // park with mail already queued
			g.SendRaw(g.Self(), tagIntrMail, nil)
			return park()
		case tagIntrBare:
			return gina.WaitIOOrMessage() // nothing staged: contract violation
		case gina.TagYield:
			rec("yield")
			return park()
		case gina.TagShutdown:
			return gina.Done()
		}
		return gina.WaitMessage()
	}
}

func TestWaitIOOrMessage(t *testing.T) {
	var log []string
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 8}, nil, intrHandler(&log))},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}}}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	h := sys.BootHandle(0, 0)
	settle := func() {
		for i := 0; i < 50; i++ {
			sys.Step()
			time.Sleep(time.Millisecond)
		}
	}
	got := func() string { s := strings.Join(log, ","); log = log[:0]; return s }

	sys.Send(h, tagIntrStart, nil)
	settle()
	start := got()
	if !strings.HasPrefix(start, "port=") {
		t.Fatalf("start: %q", start)
	}
	var port int
	fmt.Sscanf(start, "port=%d", &port)

	// A message wakes the parked accept: its completion comes first, then the mail.
	sys.Send(h, tagIntrMail, nil)
	settle()
	if g := got(); g != "interrupted,mail,yield" {
		t.Fatalf("interrupt order: %q", g)
	}

	// Mail already queued when the isolate parks interrupts the read at once. (The
	// first "interrupted" is the external tagIntrSelfMail itself waking the park.)
	sys.Send(h, tagIntrSelfMail, nil)
	settle()
	if g := got(); g != "interrupted,interrupted,mail,yield" {
		t.Fatalf("park behind mail: %q", g)
	}

	// An ordinary completion still works, and does not count as an interruption.
	fd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	defer syscall.Close(fd)
	if err := syscall.Connect(fd, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	settle()
	if g := got(); g != "accepted" {
		t.Fatalf("accept: %q", g)
	}

	// Several messages in a row are all delivered, in order.
	for i := 0; i < 3; i++ {
		sys.Send(h, tagIntrMail, nil)
	}
	settle()
	if g := got(); !strings.HasPrefix(g, "interrupted,mail,") || strings.Count(g, "mail") != 3 {
		t.Fatalf("burst: %q", g)
	}

	// Returning it with nothing staged is a contract violation.
	before := sys.Shard(0).Stats().Crashes
	sys.Send(h, tagIntrBare, nil)
	settle()
	if sys.Shard(0).Stats().Crashes != before+1 {
		t.Fatal("WaitIOOrMessage without a staged read did not crash the isolate")
	}
	if err := sys.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}
