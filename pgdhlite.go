package pgdhlite

import (
	"context"
	"errors"
	"strconv"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"

	cfg "github.com/eaglebush/config"
	std "github.com/eaglebush/stdutil"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// PostgreSQLHelper - a struct derived from datahelperlite
type PostgreSQLHelper struct {
	con    *pgx.Conn
	dbi    *cfg.DatabaseInfo
	ctx    context.Context
	tx     pgx.Tx
	rws    dhl.Rows
	trcnt  int
	reused bool
}

func init() {
	dhl.SetHelper(`pgdhlite`, &PostgreSQLHelper{})
}

// Open a new connection
func (h *PostgreSQLHelper) Open(ctx context.Context, di *cfg.DatabaseInfo) error {

	var (
		err error
	)

	if ctx == nil {
		ctx = context.Background()
	}

	h.dbi = di
	h.ctx = ctx
	h.reused = true

	if h.con == nil || h.con.IsClosed() {
		h.con, err = pgx.Connect(ctx, di.ConnectionString)
		if err != nil {
			return err
		}
		h.reused = false
	}

	return nil
}

// Close PostgreSQLHelper
func (h *PostgreSQLHelper) Close() error {

	if h.con == nil {
		return errors.New(`No connection of the object was initialized`)
	}

	if h.reused {
		return nil
	}

	if err := h.con.Close(h.ctx); err != nil {
		return err
	}

	h.trcnt = 0
	h.reused = false

	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *PostgreSQLHelper) Begin() error {

	var (
		err error
	)

	if h.con == nil {
		return errors.New(`No connection of the object was initialized`)
	}

	if h.tx == nil {
		h.tx, err = h.con.Begin(h.ctx)
		if err != nil {
			return err
		}
		h.trcnt++
	}

	return nil
}

// Commit a transaction
func (h *PostgreSQLHelper) Commit() error {

	// exit if the connection was just reused
	if h.reused {
		return nil
	}

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return errors.New(`No transaction was initialized`)
	}

	if err := h.tx.Commit(h.ctx); err != nil {
		return err
	}

	// decrement transaction
	if h.trcnt > 0 {
		h.trcnt--
	}

	// if trancount is zero, we can set the tx to nil
	if h.trcnt == 0 {
		h.tx = nil
	}

	return nil
}

// Rollback a transaction
func (h *PostgreSQLHelper) Rollback() error {

	// exit if the connection was just reused
	if h.reused {
		return nil
	}

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return errors.New(`No transaction was initialized`)
	}

	if err := h.tx.Rollback(h.ctx); err != nil {
		return err
	}

	// decrement transaction
	if h.trcnt > 0 {
		h.trcnt--
	}

	// if trancount is zero, we can set the tx to nil
	if h.trcnt == 0 {
		h.tx = nil
	}

	return nil
}

// Mark a savepoint
func (h *PostgreSQLHelper) Mark(name string) error {

	var err error

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return errors.New(`No transaction was initialized`)
	}

	_, err = h.tx.Exec(h.ctx, `SAVEPOINT sp_`+name+`;`)

	return err
}

// Discard a savepoint
func (h *PostgreSQLHelper) Discard(name string) error {
	var err error

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return errors.New(`No transaction was initialized`)
	}

	_, err = h.tx.Exec(h.ctx, `ROLLBACK TO SAVEPOINT sp_`+name+`;`)

	return err
}

// Save a savepoint
func (h *PostgreSQLHelper) Save(name string) error {
	var err error

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return errors.New(`No transaction was initialized`)
	}

	_, err = h.tx.Exec(h.ctx, `RELEASE SAVEPOINT sp_`+name+`;`)

	return err
}

// Query from PostgreSQL helper
func (h *PostgreSQLHelper) Query(sql string, args ...interface{}) (dhl.Rows, error) {

	var (
		err error
	)

	if h.tx != nil {
		h.rws, err = h.tx.Query(h.ctx, sql, args...)

		if err == nil {
			return h.rws, err
		}

		if err != pgx.ErrTxClosed {
			return h.rws, err
		}
	}

	return h.con.Query(h.ctx, sql, args...)
}

// QueryRow from PostgreSQL helper
func (h *PostgreSQLHelper) QueryRow(sql string, args ...interface{}) dhl.Row {

	if h.tx != nil {
		rw := h.tx.QueryRow(h.ctx, sql, args...)

		if rw != nil {
			return rw
		}
	}

	return h.con.QueryRow(h.ctx, sql, args...)
}

// Exec from PostgreSQL helper
func (h *PostgreSQLHelper) Exec(sql string, args ...interface{}) (int64, error) {

	var (
		err error
		ct  pgconn.CommandTag
	)

	if h.tx != nil {

		ct, err = h.tx.Exec(h.ctx, sql, args...)

		if err == nil {
			return ct.RowsAffected(), nil
		}

		if err != pgx.ErrTxClosed {
			return 0, err
		}
	}

	ct, err = h.con.Exec(h.ctx, sql, args...)
	if err != nil {
		return 0, err
	}

	return ct.RowsAffected(), nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *PostgreSQLHelper) VerifyWithin(tablename string, values []std.ValidationExpression) (Valid bool, QueryOK bool, Message string) {
	tableNameWithParameters := tablename

	args := make([]interface{}, len(values))
	i := 0
	andstr := ""
	placeholder := h.dbi.ParameterPlaceholder

	if len(values) > 0 {
		tableNameWithParameters += ` WHERE `
	}

	for _, v := range values {

		if h.dbi.ParameterInSequence {
			placeholder = h.dbi.ParameterPlaceholder + strconv.Itoa(i+1)
		}

		// If there is no operator, we default to "="
		if v.Operator == "" {
			v.Operator = "="
		}

		tableNameWithParameters += andstr + v.Name + v.Operator + placeholder
		args[i] = v.Value
		i++
		andstr = " AND "

	}

	var (
		sql    string
		exists bool
		err    error
	)

	sql = dhl.InterpolateTable(`SELECT EXISTS (SELECT 1 FROM `+tableNameWithParameters+`);`, h.dbi.Schema)

	err = h.QueryRow(sql, args...).Scan(&exists)
	if err != nil {
		return false, false, err.Error()
	}

	return exists, true, ""
}
