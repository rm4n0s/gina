package http2

// Test-only access to the HPACK codec for the raw-frame tests in package http2_test.

// TestDecoder decodes response header blocks.
type TestDecoder struct{ d *hpackDecoder }

func NewTestDecoder() *TestDecoder { return &TestDecoder{newHpackDecoder(4096, 1<<20)} }

// Decode returns the (name, value) pairs of a header block.
func (t *TestDecoder) Decode(block []byte) ([][2]string, error) {
	var out [][2]string
	err := t.d.decode(block, func(n, v []byte) { out = append(out, [2]string{string(n), string(v)}) })
	return out, err
}

// EncodeBlock encodes fields as literals (never touching the dynamic table), the
// way the server encodes responses.
func EncodeBlock(fields ...[2]string) []byte {
	var b []byte
	for _, f := range fields {
		b = appendLiteral(b, []byte(f[0]), []byte(f[1]))
	}
	return b
}

// EncodeBlockIndexed adds each field to the peer's dynamic table (incremental
// indexing) so later blocks can refer to it by index.
func EncodeBlockIndexed(fields ...[2]string) []byte {
	var b []byte
	for _, f := range fields {
		b = append(b, 0x40)
		b = appendStr(b, []byte(f[0]))
		b = appendStr(b, []byte(f[1]))
	}
	return b
}
