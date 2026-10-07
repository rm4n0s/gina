package http2

import "errors"

// HPACK (RFC 7541): header compression. The decoder implements the whole format
// (indexed fields, all three literal kinds, dynamic table size updates, Huffman
// strings). The encoder is deliberately stateless: it never inserts into the
// dynamic table, so a response header block does not depend on any earlier one.
// It uses the static table for names and for the common ":status" values and
// Huffman-codes a string only when that makes it shorter.

var (
	errHpackIndex  = errors.New("hpack: invalid index")
	errHpackInt    = errors.New("hpack: integer too large")
	errHpackTrunc  = errors.New("hpack: truncated header block")
	errHpackHuff   = errors.New("hpack: invalid Huffman data")
	errHpackSize   = errors.New("hpack: dynamic table size update too large or misplaced")
	errHpackTooBig = errors.New("hpack: string too long")
)

// ---- static table (RFC 7541 Appendix A) ----

type hpackEntry struct{ name, value []byte }

var staticSrc = [...][2]string{
	{":authority", ""}, {":method", "GET"}, {":method", "POST"}, {":path", "/"},
	{":path", "/index.html"}, {":scheme", "http"}, {":scheme", "https"}, {":status", "200"},
	{":status", "204"}, {":status", "206"}, {":status", "304"}, {":status", "400"},
	{":status", "404"}, {":status", "500"}, {"accept-charset", ""}, {"accept-encoding", "gzip, deflate"},
	{"accept-language", ""}, {"accept-ranges", ""}, {"accept", ""}, {"access-control-allow-origin", ""},
	{"age", ""}, {"allow", ""}, {"authorization", ""}, {"cache-control", ""},
	{"content-disposition", ""}, {"content-encoding", ""}, {"content-language", ""}, {"content-length", ""},
	{"content-location", ""}, {"content-range", ""}, {"content-type", ""}, {"cookie", ""},
	{"date", ""}, {"etag", ""}, {"expect", ""}, {"expires", ""},
	{"from", ""}, {"host", ""}, {"if-match", ""}, {"if-modified-since", ""},
	{"if-none-match", ""}, {"if-range", ""}, {"if-unmodified-since", ""}, {"last-modified", ""},
	{"link", ""}, {"location", ""}, {"max-forwards", ""}, {"proxy-authenticate", ""},
	{"proxy-authorization", ""}, {"range", ""}, {"referer", ""}, {"refresh", ""},
	{"retry-after", ""}, {"server", ""}, {"set-cookie", ""}, {"strict-transport-security", ""},
	{"transfer-encoding", ""}, {"user-agent", ""}, {"vary", ""}, {"via", ""},
	{"www-authenticate", ""},
}

const staticLen = len(staticSrc) // 61; dynamic entries are indexed from staticLen+1

var (
	staticTab  [staticLen + 1]hpackEntry // 1-based
	staticName = map[string]int{}        // header name -> lowest static index
)

func init() {
	for i, e := range staticSrc {
		staticTab[i+1] = hpackEntry{[]byte(e[0]), []byte(e[1])}
		if _, ok := staticName[e[0]]; !ok {
			staticName[e[0]] = i + 1
		}
	}
	buildHuffTree()
}

// ---- Huffman ----

type huffNode struct {
	next [2]int16
	sym  int16 // -1 for an interior node
}

var huffTree []huffNode

func buildHuffTree() {
	huffTree = []huffNode{{sym: -1}}
	add := func(sym int, code uint32, n uint8) {
		cur := 0
		for i := int(n) - 1; i >= 0; i-- {
			bit := (code >> uint(i)) & 1
			if huffTree[cur].next[bit] == 0 {
				huffTree = append(huffTree, huffNode{sym: -1})
				huffTree[cur].next[bit] = int16(len(huffTree) - 1)
			}
			cur = int(huffTree[cur].next[bit])
		}
		huffTree[cur].sym = int16(sym)
	}
	for s := 0; s < 256; s++ {
		add(s, huffCode[s], huffLen[s])
	}
	add(256, 0x3fffffff, 30) // EOS
}

// huffDecode appends the decoded form of src to dst. Trailing padding must be
// the most significant bits of EOS (all ones) and shorter than 8 bits, and EOS
// itself must not appear (RFC 7541 §5.2).
func huffDecode(dst, src []byte) ([]byte, error) {
	node, pad := 0, 0
	ones := true
	for _, b := range src {
		for i := 7; i >= 0; i-- {
			bit := (b >> uint(i)) & 1
			node = int(huffTree[node].next[bit])
			if s := huffTree[node].sym; s >= 0 {
				if s == 256 {
					return dst, errHpackHuff
				}
				dst = append(dst, byte(s))
				node, pad, ones = 0, 0, true
			} else {
				pad++
				ones = ones && bit == 1
			}
		}
	}
	if pad > 7 || !ones {
		return dst, errHpackHuff
	}
	return dst, nil
}

func huffEncodedLen(s []byte) int {
	n := 0
	for _, c := range s {
		n += int(huffLen[c])
	}
	return (n + 7) / 8
}

func huffAppend(dst, s []byte) []byte {
	var acc uint64
	var nbits uint
	for _, c := range s {
		acc = acc<<huffLen[c] | uint64(huffCode[c])
		nbits += uint(huffLen[c])
		for nbits >= 8 {
			nbits -= 8
			dst = append(dst, byte(acc>>nbits))
		}
	}
	if nbits > 0 { // pad with the most significant bits of EOS
		dst = append(dst, byte(acc<<(8-nbits))|byte(0xff>>nbits))
	}
	return dst
}

// ---- primitives (RFC 7541 §5) ----

// appendInt writes v with an n-bit prefix; first holds the flag bits above it.
func appendInt(dst []byte, first byte, n uint8, v uint64) []byte {
	max := uint64(1)<<n - 1
	if v < max {
		return append(dst, first|byte(v))
	}
	dst = append(dst, first|byte(max))
	v -= max
	for v >= 128 {
		dst = append(dst, byte(v&127)|128)
		v >>= 7
	}
	return append(dst, byte(v))
}

// readInt reads an integer with an n-bit prefix.
func readInt(p []byte, n uint8) (uint64, []byte, error) {
	if len(p) == 0 {
		return 0, nil, errHpackTrunc
	}
	max := uint64(1)<<n - 1
	v := uint64(p[0]) & max
	p = p[1:]
	if v < max {
		return v, p, nil
	}
	for m := uint(0); ; m += 7 {
		if len(p) == 0 {
			return 0, nil, errHpackTrunc
		}
		if m > 28 { // nothing legitimate needs more than 2^35
			return 0, nil, errHpackInt
		}
		b := p[0]
		p = p[1:]
		v += uint64(b&127) << m
		if b&128 == 0 {
			return v, p, nil
		}
	}
}

func appendStr(dst, s []byte) []byte {
	if n := huffEncodedLen(s); n < len(s) {
		dst = appendInt(dst, 0x80, 7, uint64(n))
		return huffAppend(dst, s)
	}
	dst = appendInt(dst, 0, 7, uint64(len(s)))
	return append(dst, s...)
}

// ---- encoder ----

// appendLiteral writes a header field as "literal without indexing", with the
// name taken from the static table when it is there.
func appendLiteral(dst, name, value []byte) []byte {
	if i, ok := staticName[string(name)]; ok {
		dst = appendInt(dst, 0, 4, uint64(i))
	} else {
		dst = append(dst, 0)
		dst = appendStr(dst, name)
	}
	return appendStr(dst, value)
}

// appendStatus writes the :status pseudo-header.
func appendStatus(dst []byte, code int) []byte {
	switch code {
	case 200:
		return append(dst, 0x88)
	case 204:
		return append(dst, 0x89)
	case 206:
		return append(dst, 0x8a)
	case 304:
		return append(dst, 0x8b)
	case 400:
		return append(dst, 0x8c)
	case 404:
		return append(dst, 0x8d)
	case 500:
		return append(dst, 0x8e)
	}
	dst = appendInt(dst, 0, 4, 8) // :status
	dst = append(dst, 3)          // three ASCII digits, not Huffman-coded
	return append(dst, byte('0'+code/100%10), byte('0'+code/10%10), byte('0'+code%10))
}

// ---- decoder ----

// hpackDecoder holds a connection's dynamic table. It is single-threaded like
// everything else in an isolate.
type hpackDecoder struct {
	dyn       []hpackEntry // newest first
	dynSize   int          // sum of len(name)+len(value)+32
	maxSize   int          // current table size limit (<= allowed)
	allowed   int          // the limit we advertised in SETTINGS_HEADER_TABLE_SIZE
	maxString int          // longest string accepted, decoded
	scratch   []byte
}

func newHpackDecoder(allowed, maxString int) *hpackDecoder {
	return &hpackDecoder{maxSize: allowed, allowed: allowed, maxString: maxString}
}

func (d *hpackDecoder) lookup(i uint64) (hpackEntry, bool) {
	if i == 0 {
		return hpackEntry{}, false
	}
	if i <= uint64(staticLen) {
		return staticTab[i], true
	}
	i -= uint64(staticLen) + 1
	if i >= uint64(len(d.dyn)) {
		return hpackEntry{}, false
	}
	return d.dyn[i], true
}

func (d *hpackDecoder) evictTo(limit int) {
	for d.dynSize > limit && len(d.dyn) > 0 {
		last := d.dyn[len(d.dyn)-1]
		d.dynSize -= len(last.name) + len(last.value) + 32
		d.dyn[len(d.dyn)-1] = hpackEntry{}
		d.dyn = d.dyn[:len(d.dyn)-1]
	}
}

func (d *hpackDecoder) insert(name, value []byte) {
	size := len(name) + len(value) + 32
	if size > d.maxSize { // an entry larger than the table empties it and is not added
		d.evictTo(0)
		return
	}
	d.evictTo(d.maxSize - size)
	e := hpackEntry{append([]byte(nil), name...), append([]byte(nil), value...)}
	d.dyn = append(d.dyn, hpackEntry{})
	copy(d.dyn[1:], d.dyn)
	d.dyn[0] = e
	d.dynSize += size
}

// str reads a string literal. The result aliases p or d.scratch; appending to
// d.scratch for a later string never invalidates an earlier result (a grown
// buffer is a new array and the old one stays alive).
func (d *hpackDecoder) str(p []byte) (s, rest []byte, err error) {
	if len(p) == 0 {
		return nil, nil, errHpackTrunc
	}
	huff := p[0]&0x80 != 0
	n, p, err := readInt(p, 7)
	if err != nil {
		return nil, nil, err
	}
	if n > uint64(len(p)) {
		return nil, nil, errHpackTrunc
	}
	if int(n) > d.maxString { // bounds the decoded size too: Huffman text is at most 8/5 as long
		return nil, nil, errHpackTooBig
	}
	raw := p[:n]
	if !huff {
		return raw, p[n:], nil
	}
	start := len(d.scratch)
	if d.scratch, err = huffDecode(d.scratch, raw); err != nil {
		return nil, nil, err
	}
	return d.scratch[start:], p[n:], nil
}

// decode parses one complete header block, calling emit for each field in order.
// emit's slices are only valid during the call. emit cannot abort the decode:
// the whole block must be consumed even when its fields are unwanted, to keep the
// dynamic table in step with the peer's. An error from decode is a
// COMPRESSION_ERROR: the connection must be closed.
func (d *hpackDecoder) decode(block []byte, emit func(name, value []byte)) error {
	d.scratch = d.scratch[:0]
	first := true
	for len(block) > 0 {
		b := block[0]
		switch {
		case b&0x80 != 0: // indexed field
			i, rest, err := readInt(block, 7)
			if err != nil {
				return err
			}
			e, ok := d.lookup(i)
			if !ok {
				return errHpackIndex
			}
			block = rest
			emit(e.name, e.value)
		case b&0xe0 == 0x20: // dynamic table size update: only at the start of a block
			if !first {
				return errHpackSize
			}
			n, rest, err := readInt(block, 5)
			if err != nil {
				return err
			}
			if n > uint64(d.allowed) {
				return errHpackSize
			}
			d.maxSize = int(n)
			d.evictTo(d.maxSize)
			block = rest
			continue // further size updates may follow
		default: // literal: 01 = with indexing (6-bit prefix), 0000 / 0001 = without / never (4-bit)
			prefix, index := uint8(4), b&0xc0 == 0x40
			if index {
				prefix = 6
			}
			i, rest, err := readInt(block, prefix)
			if err != nil {
				return err
			}
			d.scratch = d.scratch[:0]
			var name, value []byte
			if i == 0 {
				name, rest, err = d.str(rest)
				if err != nil {
					return err
				}
			} else {
				e, ok := d.lookup(i)
				if !ok {
					return errHpackIndex
				}
				name = e.name
			}
			value, rest, err = d.str(rest)
			if err != nil {
				return err
			}
			block = rest
			if index {
				d.insert(name, value)
			}
			emit(name, value)
		}
		first = false
	}
	return nil
}
