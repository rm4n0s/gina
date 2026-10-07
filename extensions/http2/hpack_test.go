package http2

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"
)

type field struct{ name, value string }

func decodeAll(t *testing.T, d *hpackDecoder, block []byte) ([]field, error) {
	t.Helper()
	var out []field
	err := d.decode(block, func(n, v []byte) { out = append(out, field{string(n), string(v)}) })
	return out, err
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sameFields(a, b []field) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// RFC 7541 Appendix C.4: three requests with Huffman strings on one connection,
// each relying on the dynamic table the previous ones filled.
func TestHpackRFCRequestSequence(t *testing.T) {
	d := newHpackDecoder(4096, 1<<16)
	steps := []struct {
		hex  string
		want []field
	}{
		{"8286 8441 8cf1e3c2e5f23a6ba0ab90f4ff", []field{
			{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "www.example.com"}}},
		{"8286 84be 5886a8eb10649cbf", []field{
			{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "www.example.com"}, {"cache-control", "no-cache"}}},
		{"8287 85bf 4088 25a849e95ba97d7f 8925a849e95bb8e8b4bf", []field{
			{":method", "GET"}, {":scheme", "https"}, {":path", "/index.html"}, {":authority", "www.example.com"}, {"custom-key", "custom-value"}}},
	}
	for i, st := range steps {
		got, err := decodeAll(t, d, mustHex(t, st.hex))
		if err != nil || !sameFields(got, st.want) {
			t.Fatalf("request %d: %v, %v; want %v", i+1, got, err, st.want)
		}
	}
	// C.4.3 leaves custom-key, cache-control and :authority in the table: 54+53+57 = 164 bytes
	if d.dynSize != 164 || len(d.dyn) != 3 {
		t.Fatalf("dynamic table: %d entries, %d bytes; want 3 entries, 164 bytes", len(d.dyn), d.dynSize)
	}
}

// RFC 7541 C.2.1: a literal field with incremental indexing and new name.
func TestHpackLiteralWithIndexing(t *testing.T) {
	d := newHpackDecoder(4096, 1<<16)
	got, err := decodeAll(t, d, mustHex(t, "400a637573746f6d2d6b65790d637573746f6d2d686561646572"))
	if err != nil || !sameFields(got, []field{{"custom-key", "custom-header"}}) {
		t.Fatalf("%v, %v", got, err)
	}
	if d.dynSize != 55 {
		t.Fatalf("table size %d, want 55", d.dynSize)
	}
	// and it can be referenced afterwards (index 62)
	got, err = decodeAll(t, d, []byte{0xbe})
	if err != nil || !sameFields(got, []field{{"custom-key", "custom-header"}}) {
		t.Fatalf("%v, %v", got, err)
	}
}

func TestHpackDecodeErrors(t *testing.T) {
	cases := map[string][]byte{
		"index 0":                    {0x80},
		"index past the tables":      {0xff, 0x7f},
		"truncated string":           mustHex(t, "400a6375"),
		"truncated integer":          {0xff},
		"integer overflow":           mustHex(t, "ffffffffffffff7f"),
		"size update past allowance": appendInt(nil, 0x20, 5, 5000),
		"size update not first":      {0x82, 0x20},
	}
	for name, blk := range cases {
		d := newHpackDecoder(4096, 1<<16)
		if _, err := decodeAll(t, d, blk); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestHuffmanErrors(t *testing.T) {
	bad := map[string][]byte{
		"EOS symbol":             {0xff, 0xff, 0xff, 0xff},
		"padding longer than 7":  {0x1f, 0xff}, // 'a', then 11 one-bits
		"padding that is zeros":  {0x18},       // 'a', then 000
		"padding not EOS prefix": {0x06},       // '0', then 110
		"nothing but zeros":      {0x00},
	}
	for name, src := range bad {
		if out, err := huffDecode(nil, src); err == nil {
			t.Errorf("%s decoded to %q, want an error", name, out)
		}
	}
	if out, err := huffDecode(nil, []byte{0x07}); err != nil || string(out) != "0" { // '0', then 111
		t.Errorf("valid code with 3 padding bits: %q %v", out, err)
	}
}

func TestHpackStringLimit(t *testing.T) {
	d := newHpackDecoder(4096, 8)
	block := append([]byte{0x40, 0x01, 'a', 0x09}, "123456789"...)
	if _, err := decodeAll(t, d, block); err == nil {
		t.Fatal("a 9-byte value passed an 8-byte limit")
	}
}

func TestHpackDynamicTableEviction(t *testing.T) {
	d := newHpackDecoder(100, 1<<16) // room for two small entries
	add := func(n, v string) {
		blk := []byte{0x40}
		blk = appendStr(blk, []byte(n))
		blk = appendStr(blk, []byte(v))
		if _, err := decodeAll(t, d, blk); err != nil {
			t.Fatal(err)
		}
	}
	add("aa", "11") // 36
	add("bb", "22") // 36 -> 72
	add("cc", "33") // would be 108 > 100: evicts aa
	if len(d.dyn) != 2 || string(d.dyn[0].name) != "cc" || string(d.dyn[1].name) != "bb" || d.dynSize != 72 {
		t.Fatalf("table = %v (%d bytes)", d.dyn, d.dynSize)
	}
	// an entry bigger than the whole table empties it and is not stored
	add("dd", strings.Repeat("x", 80))
	if len(d.dyn) != 0 || d.dynSize != 0 {
		t.Fatalf("oversized entry: %d entries, %d bytes", len(d.dyn), d.dynSize)
	}
	// a size update to 0 followed by one to 50 at the start of a block is legal
	got, err := decodeAll(t, d, []byte{0x20, 0x3f, 0x13, 0x82}) // 0, then 31+19=50, then :method GET
	if err != nil || !sameFields(got, []field{{":method", "GET"}}) || d.maxSize != 50 {
		t.Fatalf("%v %v max=%d", got, err, d.maxSize)
	}
}

func TestHuffmanRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		src := make([]byte, rng.Intn(64))
		for j := range src {
			if rng.Intn(2) == 0 {
				src[j] = byte('a' + rng.Intn(26))
			} else {
				src[j] = byte(rng.Intn(256))
			}
		}
		enc := huffAppend(nil, src)
		if len(enc) != huffEncodedLen(src) {
			t.Fatalf("encoded length %d, predicted %d", len(enc), huffEncodedLen(src))
		}
		dec, err := huffDecode(nil, enc)
		if err != nil || !bytes.Equal(dec, src) {
			t.Fatalf("round trip of %q: %q, %v", src, dec, err)
		}
	}
	if got := hex.EncodeToString(huffAppend(nil, []byte("www.example.com"))); got != "f1e3c2e5f23a6ba0ab90f4ff" {
		t.Fatalf("www.example.com = %s", got)
	}
}

func TestHpackIntegers(t *testing.T) {
	for _, n := range []uint8{4, 5, 6, 7} {
		for _, v := range []uint64{0, 1, 14, 15, 30, 31, 62, 63, 126, 127, 128, 1337, 65535, 1 << 20, 1<<31 - 1} {
			enc := appendInt(nil, 0, n, v)
			got, rest, err := readInt(enc, n)
			if err != nil || got != v || len(rest) != 0 {
				t.Fatalf("n=%d v=%d: got %d rest=%d err=%v (%x)", n, v, got, len(rest), err, enc)
			}
		}
	}
	// RFC 7541 C.1.2: 1337 with a 5-bit prefix is 1f 9a 0a
	if got := appendInt(nil, 0, 5, 1337); !bytes.Equal(got, []byte{0x1f, 0x9a, 0x0a}) {
		t.Fatalf("1337/5 = %x", got)
	}
}

// What the encoder writes must decode back to the same fields (and, being
// stateless, the same each time, with no dynamic table growth).
func TestHpackEncodeDecode(t *testing.T) {
	d := newHpackDecoder(4096, 1<<16)
	fields := []field{
		{"content-type", "text/plain; charset=utf-8"},
		{"content-length", "12"},
		{"date", "Wed, 07 Oct 2026 12:00:00 GMT"},
		{"server", "gina"},
		{"x-custom-header", "some value with spaces"},
		{"location", "https://example.com/a?b=c"},
		{"x-bin", "\x00\x01\xfe\xff"},
	}
	for _, status := range []int{200, 204, 206, 304, 400, 404, 500, 201, 302, 418, 503} {
		blk := appendStatus(nil, status)
		for _, f := range fields {
			blk = appendLiteral(blk, []byte(f.name), []byte(f.value))
		}
		got, err := decodeAll(t, d, blk)
		want := append([]field{{":status", itoa(status)}}, fields...)
		if err != nil || !sameFields(got, want) {
			t.Fatalf("status %d: %v, %v", status, got, err)
		}
	}
	if len(d.dyn) != 0 {
		t.Fatalf("the encoder grew the peer's dynamic table: %v", d.dyn)
	}
}

func itoa(n int) string {
	return string([]byte{byte('0' + n/100), byte('0' + n/10%10), byte('0' + n%10)})
}

// Random garbage must never panic the decoder or run it out of bounds.
func TestHpackDecodeGarbage(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 100000; i++ {
		d := newHpackDecoder(4096, 1<<12)
		blk := make([]byte, rng.Intn(48))
		rng.Read(blk)
		for j := 0; j < 3; j++ {
			d.decode(blk, func(n, v []byte) {})
		}
		if d.dynSize < 0 || d.dynSize > d.maxSize {
			t.Fatalf("table size %d outside [0,%d] after %x", d.dynSize, d.maxSize, blk)
		}
	}
}
