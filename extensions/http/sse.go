package http

import (
	"gina"
)

// Server-Sent Events. A handler calls Context.EventStream to turn its response
// into an open-ended text/event-stream. The connection isolate then stays alive
// with nothing in flight and waits for TagEvent messages: any isolate, on any
// shard, that has the connection's Handle can push to it with SendEvent.
//
// The stream ends when the client goes away (the next write fails), the server
// shuts down, or a write times out. If EventStream was given a notify handle,
// that isolate then receives TagStreamClosed with the connection's Handle as
// payload, so a publisher can drop its subscriber.
const (
	// TagEvent is delivered to a streaming connection. The payload is the event
	// data (SendEvent); the connection frames it as one or more "data:" lines.
	TagEvent gina.Tag = gina.TagUserBase + 0x10
	// TagStreamClosed is delivered to the notify isolate when a stream ends.
	// The payload is a gina.Handle (gina.PayloadAs[gina.Handle]).
	TagStreamClosed gina.Tag = gina.TagUserBase + 0x11
)

// EventStream answers the request as a Server-Sent Events stream and returns the
// handle of the connection isolate, to which SendEvent pushes events. Anything
// written to the Context before the handler returns is sent first, verbatim, so
// it should already be in event-stream format (e.g. "retry: 3000\n\n").
//
// notify, if non-zero, is told with TagStreamClosed when the stream ends.
//
// For a HEAD request there is no stream: the headers are set, no body follows,
// and EventStream returns the zero Handle. Handlers should subscribe only when
// the returned handle is non-zero.
func (c *Context) EventStream(notify gina.Handle) gina.Handle {
	c.status, c.ctype = 200, "text/event-stream"
	if c.Req.Method == "HEAD" {
		return 0
	}
	c.stream, c.notify = true, notify
	return c.g.Self()
}

// SendEvent pushes data to the SSE connection conn. data may hold newlines (each
// line becomes its own "data:" line) and must fit in one message: at most
// gina.MaxPayload (96) bytes. Delivery is best effort: a full mailbox (a client
// that cannot keep up) or a dead connection drops the event.
func SendEvent(g *gina.Ctx, conn gina.Handle, data string) gina.SendResult {
	return g.SendRaw(conn, TagEvent, []byte(data))
}

// appendEvent frames data as an SSE event: one "data:" line per line of data,
// then a blank line.
func appendEvent(w, data []byte) []byte {
	for {
		i := indexByte(data, '\n')
		line := data
		if i >= 0 {
			line = data[:i]
		}
		w = append(w, "data: "...)
		w = append(w, line...)
		w = append(w, '\n')
		if i < 0 {
			break
		}
		data = data[i+1:]
	}
	return append(w, '\n')
}
