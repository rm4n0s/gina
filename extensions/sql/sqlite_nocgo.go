//go:build !cgo

package sql

import "errors"

func sqliteOpen(string) (sqliteConn, error) {
	return nil, errors.New("SQLite needs cgo, and this binary was built with CGO_ENABLED=0")
}

func sqliteCodes(error) (primary, extended int, ok bool) { return 0, 0, false }
