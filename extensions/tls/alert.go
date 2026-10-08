package tls

import "fmt"

// Alert descriptions (RFC 8446 §6).
const (
	alertCloseNotify           = 0
	alertUnexpectedMessage     = 10
	alertBadRecordMAC          = 20
	alertRecordOverflow        = 22
	alertHandshakeFailure      = 40
	alertBadCertificate        = 42
	alertIllegalParameter      = 47
	alertDecodeError           = 50
	alertDecryptError          = 51
	alertProtocolVersion       = 70
	alertInternalError         = 80
	alertMissingExtension      = 109
	alertUnsupportedExtension  = 110
	alertNoApplicationProtocol = 120

	// alertNotTLS is internal: the peer is not speaking TLS, so no alert is sent.
	alertNotTLS = 255
)

// AlertError is a fatal TLS error; Code is the alert description sent to the peer.
type AlertError struct {
	Code uint8
	Msg  string
	Peer bool // true if the peer sent it
}

func alert(code uint8, msg string) *AlertError { return &AlertError{Code: code, Msg: msg} }

func (e *AlertError) Error() string {
	if e.Peer {
		return fmt.Sprintf("tls: peer sent alert %d: %s", e.Code, e.Msg)
	}
	return fmt.Sprintf("tls: alert %d: %s", e.Code, e.Msg)
}
