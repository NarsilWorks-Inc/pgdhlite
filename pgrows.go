package pgdhlite

import "github.com/jackc/pgx/v4"

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

	return
}

// Err check
func (ss PostgreSQLRows) Err() error {
	return ss.sqr.Err()
}

// Next row in the sequence
func (ss PostgreSQLRows) Next() bool {
	return ss.sqr.Next()
}

// Scan to destination variables
func (ss PostgreSQLRows) Scan(dest ...interface{}) error {

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

// Values from the rows
func (ss PostgreSQLRows) Values() ([]interface{}, error) {
	return nil, nil
}

// RawValues from the rows
func (ss PostgreSQLRows) RawValues() [][]byte {
	return nil
}
