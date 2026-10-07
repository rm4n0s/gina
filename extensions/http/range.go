package http

import "strconv"

// ServeBytes replies with b, honouring a single-range Range request (RFC 9110
// §14): "bytes=a-b", "bytes=a-" and "bytes=-n" answer 206 with Content-Range,
// a range outside the body answers 416, and anything else (no Range header, several
// ranges, a unit other than bytes, a failed If-Range) answers 200 with the whole
// body. Accept-Ranges is always advertised.
//
// If-Range compares against an ETag or Last-Modified header the handler has
// already set with SetHeader; with neither, a request carrying If-Range gets the
// whole body (always correct, if not the cheapest).
//
// b is copied into the response (only the requested part of it), like Bytes.
func (c *Context) ServeBytes(contentType string, b []byte) {
	c.SetHeader("Accept-Ranges", "bytes")
	rng := c.Req.Header("Range")
	if rng == nil || (c.Req.Method != "GET" && c.Req.Method != "HEAD") || !c.ifRangeHolds() {
		c.Bytes(200, contentType, b)
		return
	}
	start, end, st := parseRange(rng, int64(len(b)))
	switch st {
	case rangeIgnore:
		c.Bytes(200, contentType, b)
	case rangeBad:
		c.status, c.ctype, c.body = 416, "text/plain; charset=utf-8", append(c.body[:0], "range not satisfiable\n"...)
		c.SetHeader("Content-Range", "bytes */"+strconv.Itoa(len(b)))
	default:
		c.SetHeader("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(b)))
		c.Bytes(206, contentType, b[start:end+1])
	}
}

// ifRangeHolds reports whether the request's conditions allow a partial answer.
func (c *Context) ifRangeHolds() bool {
	ir := c.Req.Header("If-Range")
	if ir == nil {
		return true
	}
	for _, name := range []string{"ETag", "Last-Modified"} {
		if v := c.responseHeader(name); v != nil && string(v) == string(ir) && !(len(ir) >= 2 && ir[0] == 'W' && ir[1] == '/') {
			return true // a weak ETag never matches for a range (§13.1.5)
		}
	}
	return false
}

// responseHeader returns the value of an extra response header set so far.
func (c *Context) responseHeader(name string) []byte {
	h := c.hdr
	for len(h) > 0 {
		end := indexByte(h, '\r')
		if end < 0 {
			return nil
		}
		line := h[:end]
		if colon := indexByte(line, ':'); colon > 0 && equalFold(line[:colon], name) && colon+2 <= len(line) {
			return line[colon+2:]
		}
		if end+2 > len(h) {
			return nil
		}
		h = h[end+2:]
	}
	return nil
}

type rangeResult uint8

const (
	rangeOK     rangeResult = iota
	rangeIgnore             // not a single byte range: serve the whole body
	rangeBad                // unsatisfiable: 416
)

// parseRange resolves a Range header value against a body of size bytes.
func parseRange(v []byte, size int64) (start, end int64, st rangeResult) {
	const unit = "bytes="
	if len(v) < len(unit) || string(v[:len(unit)]) != unit {
		return 0, 0, rangeIgnore
	}
	spec := trimOWS(v[len(unit):])
	for _, ch := range spec {
		if ch == ',' {
			return 0, 0, rangeIgnore // several ranges: legal to answer with the whole body
		}
	}
	dash := indexByte(spec, '-')
	if dash < 0 {
		return 0, 0, rangeIgnore // malformed: ignore the header
	}
	first, last := spec[:dash], spec[dash+1:]
	switch {
	case len(first) == 0: // suffix: the last n bytes
		n, ok := parseDec(last)
		if !ok {
			return 0, 0, rangeIgnore
		}
		if n == 0 || size == 0 {
			return 0, 0, rangeBad
		}
		return max(size-n, 0), size - 1, rangeOK
	}
	a, ok := parseDec(first)
	if !ok {
		return 0, 0, rangeIgnore
	}
	b := size - 1
	if len(last) > 0 {
		var ok2 bool
		if b, ok2 = parseDec(last); !ok2 || b < a {
			return 0, 0, rangeIgnore // last before first is invalid, not unsatisfiable
		}
		b = min(b, size-1)
	}
	if a >= size {
		return 0, 0, rangeBad
	}
	return a, b, rangeOK
}

func parseDec(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 18 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}
