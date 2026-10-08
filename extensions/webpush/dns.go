package webpush

// A minimal DNS codec (RFC 1035) for the resolver isolates: build an A or AAAA
// query, read the answer. It follows no pointers, so a hostile answer cannot make
// it loop, and it trusts nothing but the message id and the echoed question.

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"strings"
)

const (
	dnsA     = 1
	dnsAAAA  = 28
	dnsOPT   = 41
	dnsIN    = 1
	dnsUDPSz = 1232 // the payload size we advertise (EDNS0), which avoids IP fragmentation

	maxAddrs = 8 // addresses kept per name
)

// dnsQuery builds a recursive query for name with an EDNS0 OPT record.
func dnsQuery(id uint16, name string, qtype uint16) ([]byte, error) {
	name = strings.TrimSuffix(name, ".")
	if name == "" || len(name) > 253 {
		return nil, errors.New("webpush: bad host name")
	}
	b := make([]byte, 12, 12+len(name)+2+4+11)
	binary.BigEndian.PutUint16(b[0:], id)
	binary.BigEndian.PutUint16(b[2:], 0x0100) // recursion desired
	binary.BigEndian.PutUint16(b[4:], 1)      // one question
	binary.BigEndian.PutUint16(b[10:], 1)     // one additional record: OPT
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 {
			return nil, errors.New("webpush: bad host name")
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				return nil, errors.New("webpush: host name must be ASCII (use punycode)")
			}
		}
		b = append(b, byte(len(label)))
		b = append(b, label...)
	}
	b = append(b, 0)
	b = binary.BigEndian.AppendUint16(b, qtype)
	b = binary.BigEndian.AppendUint16(b, dnsIN)
	b = append(b, 0) // OPT: root name,
	b = binary.BigEndian.AppendUint16(b, dnsOPT)
	b = binary.BigEndian.AppendUint16(b, dnsUDPSz) // class carries the payload size
	b = binary.BigEndian.AppendUint32(b, 0)        // extended rcode and flags
	b = binary.BigEndian.AppendUint16(b, 0)        // no options
	return b, nil
}

type dnsAnswer struct {
	rcode     int
	truncated bool
	addrs     []netip.Addr
	ttl       uint32 // smallest TTL among the addresses
}

var errDNS = errors.New("webpush: malformed DNS answer")

// dnsParse reads the answer to a query made by dnsQuery. An error means the
// message is not the answer to that query (wrong id, wrong question, garbage) and
// should be ignored.
func dnsParse(msg []byte, id uint16, name string, qtype uint16) (dnsAnswer, error) {
	var ans dnsAnswer
	if len(msg) < 12 || binary.BigEndian.Uint16(msg) != id {
		return ans, errDNS
	}
	flags := binary.BigEndian.Uint16(msg[2:])
	if flags&0x8000 == 0 || (flags>>11)&0xF != 0 { // not a response, or not a standard query's
		return ans, errDNS
	}
	ans.rcode, ans.truncated = int(flags&0xF), flags&0x0200 != 0
	if binary.BigEndian.Uint16(msg[4:]) != 1 {
		return ans, errDNS
	}
	// the question must be ours, spelled out (no compression in a question)
	off := 12
	want := strings.Split(strings.ToLower(strings.TrimSuffix(name, ".")), ".")
	for _, label := range want {
		if off >= len(msg) || int(msg[off]) != len(label) || off+1+len(label) > len(msg) ||
			strings.ToLower(string(msg[off+1:off+1+len(label)])) != label {
			return ans, errDNS
		}
		off += 1 + len(label)
	}
	if off+5 > len(msg) || msg[off] != 0 ||
		binary.BigEndian.Uint16(msg[off+1:]) != qtype || binary.BigEndian.Uint16(msg[off+3:]) != dnsIN {
		return ans, errDNS
	}
	off += 5
	if ans.rcode != 0 {
		return ans, nil
	}
	n := int(binary.BigEndian.Uint16(msg[6:]))
	ans.ttl = ^uint32(0)
	for i := 0; i < n; i++ {
		var ok bool
		if off, ok = skipName(msg, off); !ok || off+10 > len(msg) {
			return ans, errDNS
		}
		typ := binary.BigEndian.Uint16(msg[off:])
		class := binary.BigEndian.Uint16(msg[off+2:])
		ttl := binary.BigEndian.Uint32(msg[off+4:])
		rdlen := int(binary.BigEndian.Uint16(msg[off+8:]))
		off += 10
		if off+rdlen > len(msg) {
			return ans, errDNS
		}
		rdata := msg[off : off+rdlen]
		off += rdlen
		if class != dnsIN || typ != qtype || len(ans.addrs) >= maxAddrs {
			continue // CNAMEs and the like: the address records follow in the same answer
		}
		var a netip.Addr
		switch {
		case typ == dnsA && rdlen == 4:
			a = netip.AddrFrom4([4]byte(rdata))
		case typ == dnsAAAA && rdlen == 16:
			a = netip.AddrFrom16([16]byte(rdata))
		default:
			continue
		}
		ans.addrs = append(ans.addrs, a)
		ans.ttl = min(ans.ttl, ttl)
	}
	if len(ans.addrs) == 0 {
		ans.ttl = 0
	}
	return ans, nil
}

// skipName steps over a (possibly compressed) name; a pointer ends it.
func skipName(msg []byte, off int) (int, bool) {
	for {
		if off >= len(msg) {
			return 0, false
		}
		l := int(msg[off])
		switch {
		case l == 0:
			return off + 1, true
		case l&0xC0 == 0xC0:
			if off+2 > len(msg) {
				return 0, false
			}
			return off + 2, true
		case l&0xC0 != 0:
			return 0, false
		}
		off += 1 + l
	}
}

// encodeAddrs packs addresses as (length, bytes) pairs for a message.
func encodeAddrs(b []byte, addrs []netip.Addr) []byte {
	for _, a := range addrs {
		a = a.Unmap()
		b = append(b, byte(a.BitLen()/8))
		b = append(b, a.AsSlice()...)
	}
	return b
}

func decodeAddrs(b []byte) []netip.Addr {
	var out []netip.Addr
	for len(b) > 0 {
		n := int(b[0])
		if (n != 4 && n != 16) || len(b) < 1+n {
			return out
		}
		a, _ := netip.AddrFromSlice(b[1 : 1+n])
		out = append(out, a)
		b = b[1+n:]
	}
	return out
}
