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
					if err := assignUnsignedReflect(dest[i], uint64(x.Byte)); err != nil {
						return errors.New(`unhandled sql.NullByte type`)
					}
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
					if err := assignSignedReflect(dest[i], int64(x.Int32)); err != nil {
						return errors.New(`unhandled sql.NullInt32 type`)
					}
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
					if err := assignSignedReflect(dest[i], x.Int64); err != nil {
						return errors.New(`unhandled sql.NullInt64 type`)
					}
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
					if err := assignFloatReflect(dest[i], x.Float64); err != nil {
						return errors.New(`unhandled sql.NullFloat64 type`)
					}
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
					if err := assignBoolReflect(dest[i], x.Bool); err != nil {
						return errors.New(`unhandled sql.NullBool type`)
					}
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
				*s = append((*s)[:0], (*x)...)
			case *json.RawMessage:
				*s = append((*s)[:0], (*x)...)
			case **[]uint8:
				cp := append([]byte(nil), (*x)...)
				*s = &cp
			default:
				if err := assignBytesReflect(dest[i], *x); err != nil {
					return errors.New(`unhandled byte type`)
				}
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
				*s = string([]byte(*x))
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
				*s = append((*s)[:0], (*x)...)
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
	destq = make([]any, len(dest))

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

		case []uint8, *[]uint8, **[]uint8, *json.RawMessage:
			destq[i] = &[]byte{}

		case *ssd.Decimal, **ssd.Decimal:
			destq[i] = &ssd.NullDecimal{}

		case *uint8, **uint8:
			destq[i] = &sql.NullByte{}

		case any, *any:
			destq[i] = &sql.RawBytes{}

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

	// PostgreSQL smallint targets
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

func assignSignedReflect(dst any, n int64) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dst)
	}
	return setSignedReflect(rv.Elem(), n)
}

func setSignedReflect(v reflect.Value, n int64) error {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return setSignedReflect(v.Elem(), n)
	}

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.OverflowInt(n) {
			return fmt.Errorf("value %d overflows %v", n, v.Type())
		}
		v.SetInt(n)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if n < 0 {
			return fmt.Errorf("negative value %d cannot be assigned to %v", n, v.Type())
		}
		u := uint64(n)
		if v.OverflowUint(u) {
			return fmt.Errorf("value %d overflows %v", n, v.Type())
		}
		v.SetUint(u)
		return nil
	}

	return fmt.Errorf("cannot assign int64 to %v", v.Type())
}

func assignUnsignedReflect(dst any, n uint64) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dst)
	}
	return setUnsignedReflect(rv.Elem(), n)
}

func setUnsignedReflect(v reflect.Value, n uint64) error {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return setUnsignedReflect(v.Elem(), n)
	}

	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if v.OverflowUint(n) {
			return fmt.Errorf("value %d overflows %v", n, v.Type())
		}
		v.SetUint(n)
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n > uint64(^uint64(0)>>1) {
			return fmt.Errorf("value %d too large for signed assignment to %v", n, v.Type())
		}
		i := int64(n)
		if v.OverflowInt(i) {
			return fmt.Errorf("value %d overflows %v", n, v.Type())
		}
		v.SetInt(i)
		return nil
	}

	return fmt.Errorf("cannot assign uint64 to %v", v.Type())
}

func assignFloatReflect(dst any, f float64) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dst)
	}
	return setFloatReflect(rv.Elem(), f)
}

func setFloatReflect(v reflect.Value, f float64) error {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return setFloatReflect(v.Elem(), f)
	}

	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		if v.OverflowFloat(f) {
			return fmt.Errorf("value %f overflows %v", f, v.Type())
		}
		v.SetFloat(f)
		return nil
	}

	return fmt.Errorf("cannot assign float64 to %v", v.Type())
}

func assignBoolReflect(dst any, b bool) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dst)
	}
	return setBoolReflect(rv.Elem(), b)
}

func setBoolReflect(v reflect.Value, b bool) error {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return setBoolReflect(v.Elem(), b)
	}

	if v.Kind() != reflect.Bool {
		return fmt.Errorf("cannot assign bool to %v", v.Type())
	}
	v.SetBool(b)
	return nil
}

func assignBytesReflect(dst any, b []byte) error {
	rv := reflect.ValueOf(dst)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", dst)
	}

	v := rv.Elem()
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		cp := append([]byte(nil), b...)
		v.Set(reflect.ValueOf(cp).Convert(v.Type()))
		return nil
	}

	return fmt.Errorf("cannot assign []byte to %v", v.Type())
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
