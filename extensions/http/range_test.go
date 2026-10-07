//go:build linux

package http_test

import (
	"strings"
	"testing"

	ghttp "github.com/rm4n0s/gina/extensions/http"
)

func TestRangeRequests(t *testing.T) {
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	req := func(path string, hdr ...string) resp {
		lines := append([]string{"GET " + path + " HTTP/1.1", "Host: t"}, hdr...)
		return h.roundTrip(c, strings.Join(lines, "\r\n")+"\r\n\r\n")
	}
	for _, tc := range []struct {
		name, path string
		hdr        []string
		status     int
		body       string
		crange     string // expected Content-Range, "" = none
	}{
		{"no range", "/data", nil, 200, "0123456789", ""},
		{"first bytes", "/data", []string{"Range: bytes=0-3"}, 206, "0123", "bytes 0-3/10"},
		{"middle", "/data", []string{"Range: bytes=4-6"}, 206, "456", "bytes 4-6/10"},
		{"open ended", "/data", []string{"Range: bytes=7-"}, 206, "789", "bytes 7-9/10"},
		{"suffix", "/data", []string{"Range: bytes=-3"}, 206, "789", "bytes 7-9/10"},
		{"suffix larger than body", "/data", []string{"Range: bytes=-100"}, 206, "0123456789", "bytes 0-9/10"},
		{"end clamped", "/data", []string{"Range: bytes=8-100"}, 206, "89", "bytes 8-9/10"},
		{"start past the end", "/data", []string{"Range: bytes=10-"}, 416, "range not satisfiable\n", "bytes */10"},
		{"empty suffix", "/data", []string{"Range: bytes=-0"}, 416, "range not satisfiable\n", "bytes */10"},
		{"several ranges: whole body", "/data", []string{"Range: bytes=0-1,4-5"}, 200, "0123456789", ""},
		{"other unit: whole body", "/data", []string{"Range: items=0-1"}, 200, "0123456789", ""},
		{"inverted: ignored", "/data", []string{"Range: bytes=5-2"}, 200, "0123456789", ""},
		{"garbage: ignored", "/data", []string{"Range: bytes=abc"}, 200, "0123456789", ""},
		{"If-Range matching ETag", "/tagged", []string{"Range: bytes=0-1", `If-Range: "v1"`}, 206, "01", "bytes 0-1/10"},
		{"If-Range stale ETag", "/tagged", []string{"Range: bytes=0-1", `If-Range: "v0"`}, 200, "0123456789", ""},
		{"If-Range without validator", "/data", []string{"Range: bytes=0-1", `If-Range: "v1"`}, 200, "0123456789", ""},
	} {
		r := req(tc.path, tc.hdr...)
		if r.status != tc.status || r.body != tc.body {
			t.Errorf("%s: %d %q, want %d %q", tc.name, r.status, r.body, tc.status, tc.body)
		}
		if got := headerValue(r.headers, "Content-Range"); got != tc.crange {
			t.Errorf("%s: Content-Range %q, want %q", tc.name, got, tc.crange)
		}
		if headerValue(r.headers, "Accept-Ranges") != "bytes" {
			t.Errorf("%s: Accept-Ranges missing", tc.name)
		}
	}
}

func headerValue(head, name string) string {
	for _, l := range strings.Split(head, "\r\n") {
		if k, v, ok := strings.Cut(l, ":"); ok && strings.EqualFold(k, name) {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
