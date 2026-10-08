//go:build cgo

package sql

// The only file that names the SQLite driver. github.com/mattn/go-sqlite3 is a cgo
// package; without cgo (sqlite_nocgo.go) PostgreSQL still works and a SQLite pool
// fails to connect with a clear error.

import (
	"errors"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// sqliteOpen opens one connection. database/sql is not involved, so no goroutine
// is: the driver's own Open returns the raw connection.
func sqliteOpen(path string) (sqliteConn, error) {
	c, err := (&sqlite3.SQLiteDriver{}).Open(path)
	if err != nil {
		return nil, err
	}
	return c.(*sqlite3.SQLiteConn), nil
}

// sqliteCodes reads the result codes of an error from the driver.
func sqliteCodes(err error) (primary, extended int, ok bool) {
	var se sqlite3.Error
	if errors.As(err, &se) {
		return int(se.Code), int(se.ExtendedCode), true
	}
	return 0, 0, false
}
