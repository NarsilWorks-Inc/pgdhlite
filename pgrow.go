package pgdhlite

import (
	"errors"

	"github.com/jackc/pgx/v4"
)

// PostgreSQLRow struct
type PostgreSQLRow struct {
	sqr pgx.Row
}

// NewPostgreSQLRow generates a datahelper compatible PostgreSQLRow
func NewPostgreSQLRow(sqlr pgx.Row) PostgreSQLRow {
	return PostgreSQLRow{
		sqr: sqlr,
	}
}

// Scan to destination variables
func (ss PostgreSQLRow) Scan(dest ...interface{}) error {

	destq := prepareDest(dest)

	err := ss.sqr.Scan(destq...)
	if err != nil {
		return errors.New(err.Error())
	}

	// return values
	err = copyScannedToDest(dest, destq)
	if err != nil {
		return errors.New(err.Error()) // create a new error to simplify error returned
	}

	return nil

}
