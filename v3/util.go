package pgdhlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	ssd "github.com/shopspring/decimal"
)

func copyScannedToDest(dest, src []any) error {

	for i, d := range src {
		switch x := d.(type) {
		case *sql.NullString:
			if x.Valid {
				switch s := dest[i].(type) {
				case *string:
					*s = x.String
				case **string:
					*s = &x.String
				default:
					return errors.New(`unhandled sql.NullString type`)
				}
			}
		case *sql.NullByte:
			if x.Valid {
				switch s := dest[i].(type) {
				case *uint8:
					*s = x.Byte
				case **uint8:
					*s = &x.Byte
				default:
					return errors.New(`unhandled sql.NullByte type`)
				}
			}
		case *sql.NullInt16:
			if x.Valid {
				switch s := dest[i].(type) {
				case *int16:
					*s = x.Int16
				case **int16:
					*s = &x.Int16
				case *uint16:
					*s = uint16(x.Int16)
				case **uint16:
					xs := uint16(x.Int16)
					*s = &xs
				default:
					if err := assignInt16Reflect(dest[i], x.Int16); err != nil {
						return errors.New(`unhandled sql.NullInt16 type`)
					}
				}
			}
		case *sql.NullInt32:
			if x.Valid {
				switch s := dest[i].(type) {
				case *int32:
					*s = x.Int32
				case **int32:
					*s = &x.Int32
				case *int:
					*s = int(x.Int32)
				case **int:
					ic := int(x.Int32)
					*s = &ic
				case *int8:
					*s = int8(x.Int32)
				case **int8:
					ic := int8(x.Int32)
					*s = &ic
				default:
					return errors.New(`unhandled sql.NullInt32 type`)
				}
			}
		case *sql.NullInt64:
			if x.Valid {
				switch s := dest[i].(type) {
				case *int64:
					*s = x.Int64
				case **int64:
					*s = &x.Int64
				default:
					return errors.New(`unhandled sql.NullInt64 type`)
				}
			}
		case *sql.NullFloat64:
			if x.Valid {
				switch s := dest[i].(type) {
				case *float64:
					*s = x.Float64
				case **float64:
					*s = &x.Float64
				case *float32:
					*s = float32(x.Float64)
				case **float32:
					xs := float32(x.Float64)
					*s = &xs
				default:
					return errors.New(`unhandled sql.NullFloat64 type`)
				}
			}
		case *sql.NullBool:
			if x.Valid {
				switch s := dest[i].(type) {
				case *bool:
					*s = x.Bool
				case **bool:
					*s = &x.Bool
				default:
					return errors.New(`unhandled sql.NullBool type`)
				}
			}
		case *sql.NullTime:
			if x.Valid {
				switch s := dest[i].(type) {
				case *time.Time:
					*s = x.Time
				case **time.Time:
					*s = &x.Time
				default:
					return errors.New(`unhandled sql.NullTime type`)
				}
			}
		case *[]byte:
			switch s := dest[i].(type) {
			case *[]byte:
				*s = *x
			case []byte:
				copy(dest[i].(json.RawMessage), *x)
			case *json.RawMessage:
				*s = *x
			case json.RawMessage:
				s = *x
			case **[]uint8:
				*s = x
			default:
				return errors.New(`unhandled byte type`)
			}
		case *ssd.NullDecimal:
			switch s := dest[i].(type) {
			case *ssd.Decimal:
				*s = x.Decimal
			case **ssd.Decimal:
				*s = &x.Decimal
			default:
				return errors.New(`unhandled shopspring.NullDecimal type`)
			}
		case *sql.RawBytes:
			switch s := dest[i].(type) {
			case *string:
				*s = string(([]byte)(*x))
			case **string:
				if x == nil || *x == nil {
					*s = nil
					break
				}
				str := string(*x)
				if *s == nil {
					*s = new(string)
				}
				**s = str
			case *json.RawMessage:
				*s = ([]byte)(*x)
			case **json.RawMessage:
				if x == nil || *x == nil {
					*s = nil
					break
				}
				b := append([]byte(nil), (*x)...)
				rm := json.RawMessage(b)
				if *s == nil {
					*s = new(json.RawMessage)
				}
				**s = rm
			case json.RawMessage:
				s = ([]byte)(*x)
			default:
				return errors.New(`unhandled sql.RawBytes type`)
			}
		default:
			return errors.New(`unhandled sql.Null<type>`)
		}
	}

	return nil
}

func prepareDest(dest []any) (destq []any) {

	// create nullable sql destinations
	destq = make([]any, len(dest))

	// return values
	for i, d := range dest {
		switch x := d.(type) {
		case *string, **string:
			destq[i] = &sql.NullString{}
		case *int, *int8, *int32, *uint, *uint32,
			**int, **int8, **int32, **uint, **uint32:
			destq[i] = &sql.NullInt32{}
		case *int16, *uint16, **int16, **uint16:
			destq[i] = &sql.NullInt16{}
		case *int64, *uint64, **int64, **uint64:
			destq[i] = &sql.NullInt64{}
		case *float32, *float64, **float32, **float64:
			destq[i] = &sql.NullFloat64{}
		case *bool, **bool:
			destq[i] = &sql.NullBool{}
		case *time.Time, **time.Time:
			destq[i] = &sql.NullTime{}
		case []uint8, *[]uint8, **[]uint8, *json.RawMessage, json.RawMessage:
			destq[i] = &[]byte{}
		case *ssd.Decimal, **ssd.Decimal:
			destq[i] = &ssd.NullDecimal{}
		case *uint8, **uint8:
			destq[i] = &sql.NullByte{}
		default:
			destq[i] = prepareDestReflect(x)
		}
	}

	return
}

func prepareDestReflect(d any) any {
	rv := reflect.ValueOf(d)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		log.Fatal("Unhandled data type: invalid destination")
	}

	t := rv.Type().Elem()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return &sql.NullString{}
	case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16:
		return &sql.NullInt16{}
	case reflect.Int, reflect.Int32, reflect.Uint, reflect.Uint32:
		return &sql.NullInt32{}
	case reflect.Int64, reflect.Uint64:
		return &sql.NullInt64{}
	case reflect.Float32, reflect.Float64:
		return &sql.NullFloat64{}
	case reflect.Bool:
		return &sql.NullBool{}
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return &sql.NullTime{}
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return &[]byte{}
		}
	}

	log.Fatal("Unhandled data type: " + rv.Type().String())
	return nil
}

func assignInt16Reflect(dst any, n int16) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dst)
	}
	return setInt16Reflect(rv.Elem(), n)
}

func setInt16Reflect(v reflect.Value, n int16) error {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return setInt16Reflect(v.Elem(), n)
	}

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		x := int64(n)
		if v.OverflowInt(x) {
			return fmt.Errorf("value %d overflows %v", n, v.Type())
		}
		v.SetInt(x)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if n < 0 {
			return fmt.Errorf("negative value %d cannot be assigned to %v", n, v.Type())
		}
		x := uint64(n)
		if v.OverflowUint(x) {
			return fmt.Errorf("value %d overflows %v", n, v.Type())
		}
		v.SetUint(x)
		return nil
	}

	return fmt.Errorf("cannot assign int16 to %v", v.Type())
}

// isInterfaceNil checks if an interface is nil
func isInterfaceNil(i any) bool {
	if i == nil {
		return true
	}
	iv := reflect.ValueOf(i)
	if !iv.IsValid() {
		return true
	}
	switch iv.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Func, reflect.Interface:
		return iv.IsNil()
	default:
		return false
	}
}

func handlePanic(err *error) {
	if r := recover(); r != nil {
		if err != nil {
			*err = fmt.Errorf("recovered: %v", r)
		}
	}
}
