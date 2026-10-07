package http

import (
	"errors"
	"io"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"strconv"
)

// ServeFile answers with the file at path, streamed from disk in 32 KiB pieces
// (see SendReader) and with single-range support (Range, If-Range, 206/416): the
// file is never held in memory, so a download costs one buffer however large it
// is. The content type comes from the extension, Last-Modified from the file.
// A missing file or a directory answers 404, an unreadable one 403.
//
// path is used as given: never pass a client-controlled path without checking it
// (or use ServeFS, which refuses ".." by construction).
//
// Reads are blocking system calls on the shard's thread: a file in the page cache
// costs microseconds, but a cold read from a slow disk stalls every connection of
// that shard while it lasts. Serve large cold files from a dedicated shard, or
// warm them first.
func (c *Context) ServeFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		c.fileError(err)
		return
	}
	c.serveFile(f, filepath.Base(path))
}

// ServeFS is ServeFile for a file in fsys (os.DirFS, an embed.FS, ...). name is a
// slash-separated path as io/fs defines it: elements like ".." are invalid.
func (c *Context) ServeFS(fsys fs.FS, name string) {
	f, err := fsys.Open(name)
	if err != nil {
		c.fileError(err)
		return
	}
	c.serveFile(f, name)
}

func (c *Context) fileError(err error) {
	switch {
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, fs.ErrInvalid):
		c.String(404, "not found\n")
	case errors.Is(err, fs.ErrPermission):
		c.String(403, "forbidden\n")
	default:
		c.String(500, "internal server error\n")
	}
}

type fileBody struct {
	io.Reader
	f fs.File
}

func (b fileBody) Close() error { return b.f.Close() }

func (c *Context) serveFile(f fs.File, name string) {
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		f.Close()
		if err == nil {
			err = fs.ErrNotExist
		}
		c.fileError(err)
		return
	}
	ct := mime.TypeByExtension(filepath.Ext(name))
	if ct == "" {
		ct = "application/octet-stream"
	}
	size := fi.Size()
	if mt := fi.ModTime(); !mt.IsZero() && mt.Unix() > 0 {
		c.SetHeader("Last-Modified", mt.UTC().Format(timeFormat))
	}
	c.SetHeader("Accept-Ranges", "bytes")

	rng := c.Req.Header("Range")
	if rng == nil || (c.Req.Method != "GET" && c.Req.Method != "HEAD") || !c.ifRangeHolds() {
		c.SendReader(200, ct, size, fileBody{f, f})
		return
	}
	start, end, st := parseRange(rng, size)
	switch st {
	case rangeIgnore:
		c.SendReader(200, ct, size, fileBody{f, f})
	case rangeBad:
		f.Close()
		c.String(416, "range not satisfiable\n")
		c.SetHeader("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
	default:
		var r io.Reader
		switch ff := f.(type) {
		case io.ReaderAt:
			r = io.NewSectionReader(ff, start, end-start+1)
		case io.Seeker:
			if _, err := ff.Seek(start, io.SeekStart); err != nil {
				f.Close()
				c.String(500, "internal server error\n")
				return
			}
			r = io.LimitReader(f, end-start+1)
		default: // cannot seek: the whole file is always correct
			c.SendReader(200, ct, size, fileBody{f, f})
			return
		}
		c.SetHeader("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.FormatInt(size, 10))
		c.SendReader(206, ct, end-start+1, fileBody{r, f})
	}
}
