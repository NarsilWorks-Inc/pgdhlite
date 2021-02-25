package pgdhlite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"

	cfg "github.com/eaglebush/config"
	std "github.com/eaglebush/stdutil"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// PostgreSQLHelper - a struct derived from datahelperlite
type PostgreSQLHelper struct {
	//con      *pgx.Conn
	con      *pgxpool.Pool
	dbi      *cfg.DatabaseInfo
	ctx      context.Context
	tx       pgx.Tx
	rws      dhl.Rows
	rw       dhl.Row
	trcnt    int
	reusecnt int
}

func init() {
	dhl.SetHelper(`pgdhlite`, &PostgreSQLHelper{})
	dhl.SetErrNoRows(pgx.ErrNoRows)
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

	//if h.con == nil || h.con.IsClosed() {
	if h.con == nil {
		//h.con, err = pgx.Connect(ctx, di.ConnectionString)
		h.con, err = pgxpool.Connect(ctx, di.ConnectionString)
		if err != nil {
			return err
		}
		h.reusecnt = 0
	} else {
		h.reusecnt++
	}

	return nil
}

// Close PostgreSQLHelper
func (h *PostgreSQLHelper) Close() error {

	if h.con == nil {
		return dhl.ErrNoConn
	}

	// if reused, closing will be prevented
	// until reusing is zero
	if h.reusecnt > 0 {
		h.reusecnt--
		return nil
	}

	// if err := h.con.Close(h.ctx); err != nil {
	// 	return err
	// }

	// -----------------------------------------------------------
	// NOTE:
	// This part will be omitted as pgdhlite is using pgxpool
	//
	// h.con.Close()
	// -----------------------------------------------------------

	h.trcnt = 0

	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *PostgreSQLHelper) Begin() error {

	var (
		err error
	)

	if h.con == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		h.tx, err = h.con.Begin(h.ctx)
		if err != nil {
			return err
		}
	}

	h.trcnt++ // count begin transactions

	return nil
}

// Commit a transaction
func (h *PostgreSQLHelper) Commit() error {

	if h.trcnt > 1 {
		h.trcnt-- // deduct from transaction count
		return nil
	}

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return dhl.ErrNoTx
	}

	// when we get to the remaining transaction, we can commit
	if h.trcnt == 1 {
		if err := h.tx.Commit(h.ctx); err != nil {
			return err
		}
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

	if h.trcnt > 1 {
		h.trcnt-- // deduct from transaction count
		return nil
	}

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return dhl.ErrNoTx
	}

	if h.trcnt == 1 {
		if err := h.tx.Rollback(h.ctx); err != nil {
			return err
		}
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
		return dhl.ErrNoTx
	}

	// We can only mark if there was a begin
	if h.trcnt > 0 {
		_, err = h.tx.Exec(h.ctx, `SAVEPOINT sp_`+name+`;`)
	}

	return err
}

// Discard a savepoint
func (h *PostgreSQLHelper) Discard(name string) error {
	var err error

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return dhl.ErrNoTx
	}

	if h.trcnt > 0 {
		_, err = h.tx.Exec(h.ctx, `ROLLBACK TO SAVEPOINT sp_`+name+`;`)
	}

	return err
}

// Save a savepoint
func (h *PostgreSQLHelper) Save(name string) error {
	var err error

	if h.tx == nil || h.tx.Conn().IsClosed() {
		return dhl.ErrNoTx
	}

	if h.trcnt > 0 {
		_, err = h.tx.Exec(h.ctx, `RELEASE SAVEPOINT sp_`+name+`;`)
	}

	return err
}

// Query from PostgreSQL helper
func (h *PostgreSQLHelper) Query(sql string, args ...interface{}) (dhl.Rows, error) {

	var (
		err error
		sqr pgx.Rows
	)

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)

	// replace tables meant for interpolation {table} for putting the schema
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		sqr, err = h.tx.Query(h.ctx, sql, args...)
	} else {
		sqr, err = h.con.Query(h.ctx, sql, args...)
	}

	if err != nil {
		return h.rws, err
	}

	if sqr != nil {
		h.rws = NewPostgreSQLRows(&sqr)
	}

	return h.rws, nil
}

// QueryRow from PostgreSQL helper
func (h *PostgreSQLHelper) QueryRow(sql string, args ...interface{}) dhl.Row {

	var (
		sqr pgx.Row
	)

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)

	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		sqr = h.tx.QueryRow(h.ctx, sql, args...)
	} else {
		sqr = h.con.QueryRow(h.ctx, sql, args...)
	}

	if sqr != nil {
		h.rw = NewPostgreSQLRow(sqr)
	}

	return h.rw
}

// Exec from PostgreSQL helper
func (h *PostgreSQLHelper) Exec(sql string, args ...interface{}) (int64, error) {

	var (
		err error
		ct  pgconn.CommandTag
	)

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)

	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

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

// Next gets the next serial number
func (h *PostgreSQLHelper) Next(serial string, next *int64) error {

	var (
		err error
		sql string
	)

	if next == nil {
		return dhl.ErrVarMustBeInit
	}

	sql = fmt.Sprintf("SELECT nextval('%s');", h.Escape(serial))

	if h.tx != nil {
		err = h.tx.QueryRow(h.ctx, sql).Scan(next)
		if err == nil {
			return err
		}
		return nil
	}

	err = h.con.QueryRow(h.ctx, sql).Scan(next)
	if err != nil {
		return err
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *PostgreSQLHelper) VerifyWithin(tablename string, values []std.VerifyExpression) (Valid bool, QueryOK bool, Message string) {
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

		if v.Value == nil {
			v.Operator = " IS "
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
		if !errors.Is(err, dhl.ErrNoRows) {
			return false, false, err.Error()
		}
		return false, true, ""
	}

	return exists, true, ""
}

// Escape a field value (fv) from disruption by single quote
func (h *PostgreSQLHelper) Escape(fv string) string {

	if len(fv) == 0 {
		return ""
	}

	senc := *h.dbi.StringEnclosingChar
	sesc := *h.dbi.StringEscapeChar

	if len(senc) == 0 {
		senc = `'`
	}

	if len(sesc) == 0 {
		sesc = `'`
	}

	return strings.ReplaceAll(fv, senc, sesc+sesc)
}
