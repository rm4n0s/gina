package webpush

// What a resolver needs from the machine, read once by New (never by an isolate,
// whose handlers must not touch the file system).

import (
	"net/netip"
	"os"
	"strings"
)

// systemResolvers reads the nameservers of /etc/resolv.conf (at most three, like
// glibc), or falls back to the local machine.
func systemResolvers() []netip.AddrPort {
	var out []netip.AddrPort
	if data, err := os.ReadFile("/etc/resolv.conf"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			f := strings.Fields(line)
			if len(f) < 2 || f[0] != "nameserver" || len(out) == 3 {
				continue
			}
			if a, err := netip.ParseAddr(f[1]); err == nil && a.Zone() == "" { // a link-local server needs its interface: skipped
				out = append(out, netip.AddrPortFrom(a, 53))
			}
		}
	}
	if len(out) == 0 {
		out = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:53")}
	}
	return out
}

// systemHosts reads /etc/hosts, so that names such as "localhost" resolve as they
// do for every other program.
func systemHosts() map[string][]netip.Addr {
	hosts := map[string][]netip.Addr{}
	data, err := os.ReadFile("/etc/hosts")
	if err != nil {
		return hosts
	}
	for _, line := range strings.Split(string(data), "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		a, err := netip.ParseAddr(f[0])
		if err != nil || a.Zone() != "" {
			continue
		}
		for _, name := range f[1:] {
			name = strings.ToLower(name)
			hosts[name] = append(hosts[name], a.Unmap())
		}
	}
	return hosts
}
