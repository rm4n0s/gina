package sql

// Converting between Go values and PostgreSQL's text format: arguments going out
// (encodeArg) and result values coming in (convertAssign).

import (
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// OIDs of the types Scan converts by their type (the rest arrive as text).
const (
	OIDBool        = 16
	OIDBytea       = 17
	OIDInt8        = 20
	OIDInt2        = 21
	OIDInt4        = 23
	OIDText        = 25
	OIDOID         = 26
	OIDJSON        = 114
	OIDFloat4      = 700
	OIDFloat8      = 701
	OIDVarchar     = 1043
	OIDDate        = 1082
	OIDTime        = 1083
	OIDTimestamp   = 1114
	OIDTimestamptz = 1184
	OIDNumeric     = 1700
	OIDUUID        = 2950
	OIDJSONB       = 3802
)

// ---- arguments ----

// encodeArg turns a Go value into a Bind parameter. Supported: nil, bool, all
// integer and float types, string, []byte (sent in binary: bytea), time.Time,
// pointers to those (nil is NULL), types whose underlying type is one of them, and
// anything implementing driver.Valuer (so database/sql's NullString and friends
// work).
func encodeArg(v any) (param, error) {
	switch a := v.(type) {
	case nil:
		return param{null: true}, nil
	case string:
		return param{data: []byte(a)}, nil
	case []byte:
		if a == nil {
			return param{null: true}, nil
		}
		return param{format: 1, data: a, kind: kindBinary}, nil
	case bool:
		if a {
			return param{data: []byte("true"), kind: kindBool}, nil
		}
		return param{data: []byte("false"), kind: kindBool}, nil
	case int:
		return param{data: strconv.AppendInt(nil, int64(a), 10), kind: kindInt}, nil
	case int64:
		return param{data: strconv.AppendInt(nil, a, 10), kind: kindInt}, nil
	case int32:
		return param{data: strconv.AppendInt(nil, int64(a), 10), kind: kindInt}, nil
	case float64:
		return param{data: appendFloat(nil, a, 64), kind: kindFloat}, nil
	case time.Time:
		return param{data: []byte(a.Format(time.RFC3339Nano)), kind: kindTime}, nil
	case driver.Valuer:
		if rv := reflect.ValueOf(a); rv.Kind() == reflect.Pointer && rv.IsNil() {
			return param{null: true}, nil
		}
		dv, err := a.Value()
		if err != nil {
			return param{}, err
		}
		if _, again := dv.(driver.Valuer); again {
			return param{}, errors.New("driver.Valuer returned a driver.Valuer")
		}
		return encodeArg(dv)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return param{null: true}, nil
		}
		return encodeArg(rv.Elem().Interface())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return param{data: strconv.AppendInt(nil, rv.Int(), 10), kind: kindInt}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if u := rv.Uint(); u <= math.MaxInt64 {
			return param{data: strconv.AppendUint(nil, u, 10), kind: kindInt}, nil
		}
		return param{data: strconv.AppendUint(nil, rv.Uint(), 10)}, nil // text: SQLite has no unsigned 64-bit integer
	case reflect.Float32:
		return param{data: appendFloat(nil, rv.Float(), 32), kind: kindFloat}, nil
	case reflect.Float64:
		return param{data: appendFloat(nil, rv.Float(), 64), kind: kindFloat}, nil
	case reflect.Bool:
		return encodeArg(rv.Bool())
	case reflect.String:
		return param{data: []byte(rv.String())}, nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if rv.IsNil() {
				return param{null: true}, nil
			}
			return param{format: 1, data: rv.Bytes(), kind: kindBinary}, nil
		}
	}
	return param{}, fmt.Errorf("unsupported argument type %T", v)
}

func appendFloat(b []byte, f float64, bits int) []byte {
	switch {
	case math.IsNaN(f):
		return append(b, "NaN"...)
	case math.IsInf(f, 1):
		return append(b, "Infinity"...)
	case math.IsInf(f, -1):
		return append(b, "-Infinity"...)
	}
	return strconv.AppendFloat(b, f, 'g', -1, bits)
}

// ---- results ----

var errNull = errors.New("NULL cannot be stored in this type; use a pointer or a Null type")

// convertAssign stores the text value src (nil for NULL) of a column of type oid in
// the destination dest.
func convertAssign(dest any, src []byte, oid uint32) error {
	switch d := dest.(type) {
	case *any:
		v, err := anyValue(src, oid)
		*d = v
		return err
	case *string:
		if src == nil {
			return errNull
		}
		*d = string(src)
		return nil
	case *[]byte:
		if src == nil {
			*d = nil
			return nil
		}
		b, err := bytesValue(src, oid)
		*d = b
		return err
	case *bool:
		if src == nil {
			return errNull
		}
		b, err := parseBool(src)
		*d = b
		return err
	case *int:
		return assignInt(src, 64, func(v int64) { *d = int(v) })
	case *int8:
		return assignInt(src, 8, func(v int64) { *d = int8(v) })
	case *int16:
		return assignInt(src, 16, func(v int64) { *d = int16(v) })
	case *int32:
		return assignInt(src, 32, func(v int64) { *d = int32(v) })
	case *int64:
		return assignInt(src, 64, func(v int64) { *d = v })
	case *uint:
		return assignUint(src, 64, func(v uint64) { *d = uint(v) })
	case *uint8:
		return assignUint(src, 8, func(v uint64) { *d = uint8(v) })
	case *uint16:
		return assignUint(src, 16, func(v uint64) { *d = uint16(v) })
	case *uint32:
		return assignUint(src, 32, func(v uint64) { *d = uint32(v) })
	case *uint64:
		return assignUint(src, 64, func(v uint64) { *d = v })
	case *float32:
		if src == nil {
			return errNull
		}
		f, err := strconv.ParseFloat(string(src), 32)
		*d = float32(f)
		return err
	case *float64:
		if src == nil {
			return errNull
		}
		f, err := strconv.ParseFloat(string(src), 64)
		*d = f
		return err
	case *time.Time:
		if src == nil {
			return errNull
		}
		t, err := parseTime(string(src), oid)
		*d = t
		return err
	case interface{ Scan(any) error }:
		v, err := anyValue(src, oid)
		if err != nil {
			return err
		}
		return d.Scan(v)
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("destination %T is not a non-nil pointer", dest)
	}
	el := rv.Elem()
	if el.Kind() == reflect.Pointer { // **T: nil for NULL
		if src == nil {
			el.SetZero()
			return nil
		}
		if el.IsNil() {
			el.Set(reflect.New(el.Type().Elem()))
		}
		return convertAssign(el.Interface(), src, oid)
	}
	// A named type whose underlying type is one of the above.
	switch el.Kind() {
	case reflect.String:
		if src == nil {
			return errNull
		}
		el.SetString(string(src))
		return nil
	case reflect.Bool:
		if src == nil {
			return errNull
		}
		b, err := parseBool(src)
		el.SetBool(b)
		return err
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return assignInt(src, el.Type().Bits(), func(v int64) { el.SetInt(v) })
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return assignUint(src, el.Type().Bits(), func(v uint64) { el.SetUint(v) })
	case reflect.Float32, reflect.Float64:
		if src == nil {
			return errNull
		}
		f, err := strconv.ParseFloat(string(src), el.Type().Bits())
		el.SetFloat(f)
		return err
	case reflect.Slice:
		if el.Type().Elem().Kind() == reflect.Uint8 {
			if src == nil {
				el.SetZero()
				return nil
			}
			b, err := bytesValue(src, oid)
			el.SetBytes(b)
			return err
		}
	}
	return fmt.Errorf("unsupported destination type %T", dest)
}

func assignInt(src []byte, bits int, set func(int64)) error {
	if src == nil {
		return errNull
	}
	v, err := strconv.ParseInt(string(src), 10, bits)
	if err != nil {
		return err
	}
	set(v)
	return nil
}

func assignUint(src []byte, bits int, set func(uint64)) error {
	if src == nil {
		return errNull
	}
	v, err := strconv.ParseUint(string(src), 10, bits)
	if err != nil {
		return err
	}
	set(v)
	return nil
}

func parseBool(src []byte) (bool, error) {
	switch strings.ToLower(string(src)) {
	case "t", "true", "1", "y", "yes", "on":
		return true, nil
	case "f", "false", "0", "n", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("cannot read %q as a boolean", src)
}

// bytesValue copies src; a bytea in the default "hex" output format is decoded.
func bytesValue(src []byte, oid uint32) ([]byte, error) {
	if oid == OIDBytea {
		h, ok := strings.CutPrefix(string(src), `\x`)
		if !ok {
			return nil, errors.New("bytea is not in hex format (bytea_output = 'escape' is not supported)")
		}
		return hex.DecodeString(h)
	}
	return append([]byte{}, src...), nil
}

// anyValue is the natural Go value of a column: int64, float64, bool, []byte,
// time.Time or string, nil for NULL.
func anyValue(src []byte, oid uint32) (any, error) {
	if src == nil {
		return nil, nil
	}
	switch oid {
	case OIDBool:
		return parseBool(src)
	case OIDInt2, OIDInt4, OIDInt8, OIDOID:
		return strconv.ParseInt(string(src), 10, 64)
	case OIDFloat4:
		return strconv.ParseFloat(string(src), 32)
	case OIDFloat8:
		return strconv.ParseFloat(string(src), 64)
	case OIDBytea:
		return bytesValue(src, oid)
	case OIDDate, OIDTimestamp, OIDTimestamptz:
		return parseTime(string(src), oid)
	}
	return string(src), nil
}

var timeLayouts = map[uint32][]string{
	OIDDate:        {"2006-01-02"},
	OIDTime:        {"15:04:05.999999999"},
	OIDTimestamp:   {"2006-01-02 15:04:05.999999999"},
	OIDTimestamptz: {"2006-01-02 15:04:05.999999999Z07:00:00", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07"},
}

func parseTime(s string, oid uint32) (time.Time, error) {
	layouts := timeLayouts[oid]
	if layouts == nil { // text, or a type we do not know: try them all
		layouts = append(append(append(timeLayouts[OIDTimestamptz], timeLayouts[OIDTimestamp]...), timeLayouts[OIDDate]...), timeLayouts[OIDTime]...)
	}
	if strings.HasSuffix(s, " BC") || s == "infinity" || s == "-infinity" {
		return time.Time{}, fmt.Errorf("cannot read %q as a time.Time", s)
	}
	var err error
	for _, l := range layouts {
		var t time.Time
		if t, err = time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, err
}
