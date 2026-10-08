//go:build linux

package gina_test

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

const tagDialGo = gina.TagUserBase + 0x50

type dialArgs struct {
	Port    uint16
	UDP     uint8
	Timeout uint8 // connect timeout in 10 ms units, 0 = none
}

// dialIso dials 127.0.0.1:Port, waits for the connect, sends "ping" and logs the reply.
type dialIso struct {
	fd  gina.FDHandle
	buf [64]byte
	udp bool
}

func dialHandler(log *[]string) gina.Handler[dialIso] {
	return func(d *dialIso, g *gina.Ctx, m *gina.Message) gina.Effect {
		rec := func(s string) { *log = append(*log, s) }
		switch m.Tag {
		case tagDialGo:
			a := gina.PayloadAs[dialArgs](m)
			fd, err := g.Dial(gina.DialSpec{IP: netip.MustParseAddr("127.0.0.1"), Port: a.Port, UDP: a.UDP == 1})
			if err != nil {
				rec("dial:" + err.Error())
				return gina.Done()
			}
			if _, err := g.Dial(gina.DialSpec{IP: netip.MustParseAddr("127.0.0.1"), Port: a.Port}); err == nil {
				rec("second dial allowed")
			}
			d.fd, d.udp = fd, a.UDP == 1
			if peer, ok := g.PeerAddr(fd); !ok || peer.Port() != a.Port {
				rec("peer wrong")
			}
			g.IOConnect(fd, time.Duration(a.Timeout)*10*time.Millisecond)
			return gina.WaitIO()
		case gina.TagIOConnect:
			if r := gina.PayloadAs[gina.IOResult](m).Result; r != 0 {
				rec(fmt.Sprintf("connect:%v", syscall.Errno(-r)))
				return gina.Done()
			}
			rec("connected")
			g.IOSend(d.fd, []byte("ping"), 0)
			return gina.WaitIO()
		case gina.TagIOSend:
			if r := gina.PayloadAs[gina.IOResult](m).Result; r != 4 {
				rec(fmt.Sprintf("send:%d", r))
				return gina.Done()
			}
			g.IORecv(d.fd, d.buf[:], time.Second)
			return gina.WaitIO()
		case gina.TagIORecv:
			r := gina.PayloadAs[gina.IOResult](m).Result
			if r < 0 {
				rec(fmt.Sprintf("recv:%v", syscall.Errno(-r)))
			} else {
				rec("got:" + string(d.buf[:r]))
			}
			return gina.Done()
		case gina.TagShutdown:
			return gina.Done()
		}
		return gina.WaitMessage()
	}
}

func runDial(t *testing.T, a dialArgs) string {
	t.Helper()
	var log []string
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 8}, nil, dialHandler(&log))},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}}}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	h := sys.BootHandle(0, 0)
	sys.Send(h, tagDialGo, gina.BytesOf(&a))
	for i := 0; i < 300 && (len(log) == 0 || log[len(log)-1] == "connected"); i++ {
		sys.Step()
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		sys.Step()
		time.Sleep(time.Millisecond)
	}
	if err := sys.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(log, ",")
}

func TestDialTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				b := make([]byte, 16)
				n, _ := c.Read(b)
				c.Write([]byte("pong:" + string(b[:n])))
			}()
		}
	}()
	got := runDial(t, dialArgs{Port: uint16(ln.Addr().(*net.TCPAddr).Port)})
	if got != "connected,got:pong:ping" {
		t.Fatalf("log: %q", got)
	}
}

func TestDialUDP(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	go func() {
		b := make([]byte, 64)
		for {
			n, from, err := pc.ReadFrom(b)
			if err != nil {
				return
			}
			pc.WriteTo(append([]byte("pong:"), b[:n]...), from)
		}
	}()
	got := runDial(t, dialArgs{Port: uint16(pc.LocalAddr().(*net.UDPAddr).Port), UDP: 1})
	if got != "connected,got:pong:ping" {
		t.Fatalf("log: %q", got)
	}
}

func TestDialRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(ln.Addr().(*net.TCPAddr).Port)
	ln.Close() // nothing listens there any more
	got := runDial(t, dialArgs{Port: port})
	if got != "connect:connection refused" {
		t.Fatalf("log: %q", got)
	}
}

func TestDialBadAddress(t *testing.T) {
	var log []string
	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 8}, nil,
			func(_ *dialIso, g *gina.Ctx, m *gina.Message) gina.Effect {
				if m.Tag == tagDialGo {
					for _, s := range []gina.DialSpec{{}, {IP: netip.MustParseAddr("0.0.0.0"), Port: 80}, {IP: netip.MustParseAddr("127.0.0.1")}, {IP: netip.MustParseAddr("fe80::1%lo"), Port: 80}} {
						if _, err := g.Dial(s); err == nil {
							log = append(log, fmt.Sprintf("accepted %+v", s))
						}
					}
				}
				return gina.WaitMessage()
			})},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}}}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	sys.Send(sys.BootHandle(0, 0), tagDialGo, nil)
	for i := 0; i < 5; i++ {
		sys.Step()
	}
	if len(log) != 0 {
		t.Fatal(log)
	}
}
