package pgdhlite

import (
	"reflect"

	"github.com/NarsilWorks-Inc/datahelperlite"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
)

// PostgreSQLRows struct
type PostgreSQLRows struct {
	sqr pgx.Rows
}

// NewPostgreSQLRows generates a datahelper compatible PostgreSQLRows
func NewPostgreSQLRows(sqlr *pgx.Rows) PostgreSQLRows {
	return PostgreSQLRows{
		sqr: *sqlr,
	}
}

// Close rows
func (ss PostgreSQLRows) Close() {
	if ss.sqr != nil {
		ss.sqr.Close()
	}
}

// Err check
func (ss PostgreSQLRows) Err() error {
	ss.sqr.FieldDescriptions()
	return ss.sqr.Err()
}

// Next row in the sequence
func (ss PostgreSQLRows) Next() bool {
	return ss.sqr.Next()
}

// Scan to destination variables
func (ss PostgreSQLRows) Scan(dest ...any) error {
	destq := prepareDest(dest)
	err := ss.sqr.Scan(destq...)
	if err != nil {
		return err
	}
	// return values
	err = copyScannedToDest(dest, destq)
	if err != nil {
		return err
	}
	return nil
}

// Columns from the rows
func (ss PostgreSQLRows) Columns() ([]datahelperlite.Column, error) {

	pgci := pgtype.NewConnInfo()
	fds := ss.sqr.FieldDescriptions()

	ctps := make([]datahelperlite.Column, len(fds))
	for i, ct := range fds {
		dt, _ := pgci.DataTypeForOID(ct.DataTypeOID)
		ctps[i] = Column{
			name:    string(ct.Name),
			dbtname: dt.Name,
			scntyp:  reflect.TypeOf(dt.Value),
		}
	}

	return ctps, nil
}

// Values from the rows
func (ss PostgreSQLRows) Values() ([]any, error) {
	return nil, nil
}

// RawValues from the rows
func (ss PostgreSQLRows) RawValues() [][]byte {
	return nil
}
