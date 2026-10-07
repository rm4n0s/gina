//go:build linux

package http_test

import (
	"net/netip"
	"syscall"
	"testing"

	ghttp "github.com/rm4n0s/gina/extensions/http"
)

func TestRemoteAddr(t *testing.T) {
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	sa, err := syscall.Getsockname(c)
	if err != nil {
		t.Fatal(err)
	}
	local := sa.(*syscall.SockaddrInet4)
	r := h.roundTrip(c, get("/ip"))
	want := netip.AddrPortFrom(netip.AddrFrom4(local.Addr), uint16(local.Port)).String()
	if r.status != 200 || r.body != want {
		t.Fatalf("RemoteAddr = %d %q, want %q", r.status, r.body, want)
	}
}

// An IPv6 listener on "::" is dual-stack: IPv4 clients arrive as plain IPv4
// addresses, IPv6 clients as IPv6.
func TestIPv6DualStackListener(t *testing.T) {
	h := start(t, ghttp.Config{IP: netip.IPv6Unspecified()}, 1)
	port := int(h.srv.Port(0))

	c4 := h.dial(0)
	r := h.roundTrip(c4, get("/ip"))
	if a, err := netip.ParseAddrPort(r.body); err != nil || !a.Addr().Is4() || r.status != 200 {
		t.Fatalf("IPv4 client: %d %q (%v)", r.status, r.body, err)
	}

	fd, err := syscall.Socket(syscall.AF_INET6, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Skip("no IPv6:", err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	loop := netip.IPv6Loopback().As16()
	if err := syscall.Connect(fd, &syscall.SockaddrInet6{Port: port, Addr: loop}); err != nil && err != syscall.EINPROGRESS {
		t.Skip("no IPv6 loopback:", err)
	}
	r = h.roundTrip(fd, get("/ip"))
	if a, err := netip.ParseAddrPort(r.body); err != nil || !a.Addr().Is6() || r.status != 200 {
		t.Fatalf("IPv6 client: %d %q (%v)", r.status, r.body, err)
	}
}
