package http2

// Frame layer (RFC 9113 §4, §6).

const (
	frameData         = 0x0
	frameHeaders      = 0x1
	framePriority     = 0x2
	frameRSTStream    = 0x3
	frameSettings     = 0x4
	framePushPromise  = 0x5
	framePing         = 0x6
	frameGoAway       = 0x7
	frameWindowUpdate = 0x8
	frameContinuation = 0x9
)

const (
	flagEndStream  = 0x1 // DATA, HEADERS
	flagAck        = 0x1 // SETTINGS, PING
	flagEndHeaders = 0x4 // HEADERS, CONTINUATION
	flagPadded     = 0x8 // DATA, HEADERS
	flagPriority   = 0x20
)

// Error codes (RFC 9113 §7).
type errCode uint32

const (
	errNo                 errCode = 0x0
	errProtocol           errCode = 0x1
	errInternal           errCode = 0x2
	errFlowControl        errCode = 0x3
	errSettingsTimeout    errCode = 0x4
	errStreamClosed       errCode = 0x5
	errFrameSize          errCode = 0x6
	errRefusedStream      errCode = 0x7
	errCancel             errCode = 0x8
	errCompression        errCode = 0x9
	errEnhanceYourCalm    errCode = 0xb
	errInadequateSecurity errCode = 0xc
)

// Settings identifiers (RFC 9113 §6.5.2).
const (
	setHeaderTableSize       = 0x1
	setEnablePush            = 0x2
	setMaxConcurrentStreams  = 0x3
	setInitialWindowSize     = 0x4
	setMaxFrameSize          = 0x5
	setMaxHeaderListSize     = 0x6
	setEnableConnectProtocol = 0x8 // RFC 8441: the server accepts extended CONNECT
)

const (
	frameHeaderLen = 9
	maxWindow      = 1<<31 - 1
	defaultWindow  = 65535
)

const clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// beginFrame appends a frame header with a zero length and returns its offset;
// endFrame fills the length in once the payload has been appended.
func beginFrame(dst []byte, typ, flags byte, id uint32) ([]byte, int) {
	at := len(dst)
	return append(dst, 0, 0, 0, typ, flags, byte(id>>24)&0x7f, byte(id>>16), byte(id>>8), byte(id)), at
}

func endFrame(dst []byte, at int) {
	n := len(dst) - at - frameHeaderLen
	dst[at], dst[at+1], dst[at+2] = byte(n>>16), byte(n>>8), byte(n)
}

func appendU32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendRST(dst []byte, id uint32, code errCode) []byte {
	dst, _ = beginFrame(dst, frameRSTStream, 0, id)
	dst[len(dst)-frameHeaderLen+2] = 4 // length: always 4
	return appendU32(dst, uint32(code))
}

func appendWindowUpdate(dst []byte, id uint32, inc uint32) []byte {
	dst, _ = beginFrame(dst, frameWindowUpdate, 0, id)
	dst[len(dst)-frameHeaderLen+2] = 4
	return appendU32(dst, inc)
}

func appendGoAway(dst []byte, last uint32, code errCode) []byte {
	dst, _ = beginFrame(dst, frameGoAway, 0, 0)
	dst[len(dst)-frameHeaderLen+2] = 8
	dst = appendU32(dst, last)
	return appendU32(dst, uint32(code))
}

func appendSetting(dst []byte, id uint16, v uint32) []byte {
	dst = append(dst, byte(id>>8), byte(id))
	return appendU32(dst, v)
}
