package pgdhlite

import (
	"github.com/jackc/pgx/v5"
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
func (ss PostgreSQLRow) Scan(dest ...any) error {

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
