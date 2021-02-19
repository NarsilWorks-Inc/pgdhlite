package pgdhlite

import (
	"database/sql"
	"errors"
	"log"
	"reflect"
	"time"
)

func copyScannedToDest(dest, src []interface{}) error {

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
					return errors.New(`Unhandled sql.NullString type`)
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
				default:
					return errors.New(`Unhandled sql.NullInt32 type`)
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
					return errors.New(`Unhandled sql.NullInt64 type`)
				}
			}
		case *sql.NullFloat64:
			if x.Valid {
				switch s := dest[i].(type) {
				case *float64:
					*s = x.Float64
				case **float64:
					*s = &x.Float64
				default:
					return errors.New(`Unhandled sql.NullFloat64 type`)
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
					return errors.New(`Unhandled sql.NullBool type`)
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
					return errors.New(`Unhandled sql.NullTime type`)
				}
			}
		default:
			return errors.New(`Unhandled sql.Null<type>`)
		}
	}

	return nil
}

func prepareDest(dest []interface{}) (destq []interface{}) {

	// create nullable sql destinations
	destq = make([]interface{}, len(dest))

	// return values
	for i, d := range dest {
		switch x := d.(type) {
		case *string, **string:

			destq[i] = &sql.NullString{}
		case *int, *int8, *int16, *int32, *uint, *uint8, *uint16, *uint32,
			**int, **int8, **int16, **int32, **uint, **uint8, **uint16, **uint32:

			destq[i] = &sql.NullInt32{}
		case *int64, *uint64,
			**int64, **uint64:

			destq[i] = &sql.NullInt64{}
		case *float32, *float64, **float32, **float64:

			destq[i] = &sql.NullFloat64{}
		case *bool, **bool:

			destq[i] = &sql.NullBool{}
		case *time.Time, **time.Time:

			destq[i] = &sql.NullTime{}
		default:
			log.Fatal("Unhandled data type: " + reflect.TypeOf(x).Name())
		}
	}

	return
}
