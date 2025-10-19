package pgdhlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v2"
	dn "github.com/eaglebush/datainfo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLHelper implements DataHelperLite
type PostgreSQLHelper struct {
	conn *pgxpool.Pool
	dbi  *dn.DataInfo
	ctx  context.Context
	tx   pgx.Tx
	rws  dhl.Rows
	trCnt,
	reuseCnt uint8
	rw  sync.RWMutex
	err error
	rollbackTriggered,
	committed, poolAtInit bool
	trnIdMap  map[int8]bool
	lastTrnId int8
}

func init() {
	dhl.SetHelper(`pgdhlite`, &PostgreSQLHelper{})
	dhl.SetErrNoRows(pgx.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *PostgreSQLHelper) NewHelper() dhl.DataHelperLite {
	return &PostgreSQLHelper{}
}

// Open a new connection
func (h *PostgreSQLHelper) Open(ctx context.Context, di *dn.DataInfo) error {
	if h.conn != nil {
		h.rw.Lock()
		h.reuseCnt++
		h.rw.Unlock()
		return nil
	}

	h.err = nil
	if ctx == nil {
		ctx = context.Background()
	}
	h.dbi = di
	h.ctx = ctx

	var cfg *pgxpool.Config
	cfg, h.err = pgxpool.ParseConfig(*di.ConnectionString)
	if h.err != nil {
		h.err = fmt.Errorf("open: %w", h.err)
		return h.err
	}
	if di.MaxOpenConnection != nil {
		cfg.MaxConns = int32(*di.MaxOpenConnection)
	}
	if di.MaxConnectionLifetime != nil {
		cfg.MaxConnLifetime = time.Duration(*di.MaxConnectionLifetime)
	}
	if di.MaxConnectionIdleTime != nil {
		cfg.MaxConnIdleTime = time.Duration(*di.MaxConnectionIdleTime)
	}
	h.conn, h.err = pgxpool.NewWithConfig(ctx, cfg)
	if h.err != nil {
		h.err = fmt.Errorf("open: %w", h.err)
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("open: failed to create pool")
		return h.err
	}
	h.rw.Lock()
	h.reuseCnt = 0
	h.rw.Unlock()
	return nil
}

// Close PostgreSQLHelper
func (h *PostgreSQLHelper) Close() error {
	if h.conn == nil {
		return nil
	}
	// if reused, closing will be prevented
	// until reusing is zero
	if h.reuseCnt > 0 {
		h.rw.Lock()
		h.reuseCnt--
		h.rw.Unlock()
		return nil
	}
	// check if transaction exists
	if h.tx != nil {
		h.Rollback()
	}

	if h.poolAtInit {
		return nil
	}

	h.conn.Close()
	h.conn = nil

	h.rw.Lock()
	h.trCnt = 0
	h.err = nil
	h.rw.Unlock()
	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *PostgreSQLHelper) Begin() error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("begin: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.tx, h.err = h.conn.BeginTx(h.ctx, pgx.TxOptions{})
		if h.err != nil {
			h.err = fmt.Errorf("begin: %w", h.err)
			return h.err
		}
	}
	// Increment transaction count
	// The transaction count will serve as the key for the new map value, set to 1
	// Move the new index to the forward position
	h.rw.Lock()
	defer h.rw.Unlock()
	h.trCnt++
	h.committed = false         // ? Reset commit state
	h.rollbackTriggered = false // ? Reset rollback state

	// Set trn id flag up
	if h.trCnt > 1 {
		if h.trnIdMap == nil {
			h.trnIdMap = make(map[int8]bool)
		}
		h.lastTrnId++
		h.trnIdMap[h.lastTrnId] = true
	}
	return nil
}

// Begin a transaction to support deferred rollback.
func (h *PostgreSQLHelper) BeginManually() error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("begin: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.tx, h.err = h.conn.BeginTx(h.ctx, pgx.TxOptions{})
		if h.err != nil {
			h.err = fmt.Errorf("begin: %w", h.err)
			return h.err
		}
	}
	// Increment transaction count
	h.rw.Lock()
	defer h.rw.Unlock()
	h.trCnt++
	h.committed = false         // Reset commit state
	h.rollbackTriggered = false // Reset rollback state
	h.lastTrnId = 0
	h.trnIdMap = nil
	return nil
}

// Commit a transaction.
func (h *PostgreSQLHelper) Commit() error {
	// Return early if any of the conditions are true
	if h.tx == nil || h.trCnt == 0 || h.rollbackTriggered || h.committed {
		return nil
	}

	// If there is an error, we give the control to rollback
	if h.err != nil {
		return h.Rollback()
	}

	h.rw.Lock()
	defer h.rw.Unlock()

	// If the transaction is not the outermost transaction, reduce transaction count.
	if h.trCnt > 1 {
		// If this transaction was called with Begin(), this is a deferred rollback
		// Record the last transaction id (via count) and set the map to false
		// Then reduce the number of transaction count
		if h.trnIdMap != nil {
			h.trnIdMap[h.lastTrnId] = false
		}
		h.trCnt--
		return nil
	}

	// Ensure DB, connection, and transaction are valid before committing
	if h.conn == nil {
		h.err = fmt.Errorf("commit: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		h.err = fmt.Errorf("commit: %w", dhl.ErrNoTx)
		return h.err
	}

	// Commit the outermost transaction
	if h.err = h.tx.Commit(h.ctx); h.err != nil && !errors.Is(h.err, sql.ErrTxDone) {
		h.err = fmt.Errorf("commit: %w", h.err)
		return h.err
	}

	// Reset transaction state after a successful commit
	h.committed = true
	h.tx = nil
	h.trCnt = 0
	h.lastTrnId = 0
	h.trnIdMap = nil
	h.rollbackTriggered = false

	return nil
}

// Rollback a transaction.
func (h *PostgreSQLHelper) Rollback() error {

	// Return early if any of the conditions are true
	if h.tx == nil || h.trCnt == 0 || h.committed {
		return nil
	}

	if h.err != nil {
		return h.rollbk()
	}

	// If trnId's flag was off, return early
	// This only applies to deferred rollbacks
	if h.trnIdMap != nil && !h.trnIdMap[h.lastTrnId] {
		h.lastTrnId--
		return nil
	}

	// If the transaction is not the first transaction, reduce the transaction count
	if h.trCnt > 1 {
		h.trCnt--
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	return h.rollbk()
}

func (h *PostgreSQLHelper) rollbk() error {
	if h.committed {
		return nil // ?? If already committed, skip rollback
	}

	// Ensure DB, connection, and transaction are valid before rolling back
	if h.conn == nil {
		h.err = fmt.Errorf("rollback: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		h.err = fmt.Errorf("rollback: %w", dhl.ErrNoTx)
		return h.err
	}

	h.rw.Lock()
	h.rollbackTriggered = true // ?? Mark rollback occurred
	h.rw.Unlock()

	// Perform rollback
	if h.err = h.tx.Rollback(h.ctx); h.err != nil && !errors.Is(h.err, sql.ErrTxDone) {
		h.err = fmt.Errorf("rollback: %w", h.err)
		return h.err
	}

	// Reset all transaction state after rollback
	h.rw.Lock()
	defer h.rw.Unlock()
	h.tx = nil
	h.trCnt = 0
	h.committed = false         // ?? Reset flags
	h.rollbackTriggered = false // ?? Reset flags (rollback is done)
	h.trnIdMap = nil
	h.err = nil
	return nil
}

// Mark a savepoint
func (h *PostgreSQLHelper) Mark(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("mark: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.err = fmt.Errorf("mark: %w", dhl.ErrNoTx)
		return h.err
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.Exec(h.ctx, `SAVEPOINT sp_`+name+`;`)
		if h.err != nil {
			h.err = fmt.Errorf("mark: %w", h.err)
			return h.err
		}
	}
	return nil
}

// Discard a savepoint
func (h *PostgreSQLHelper) Discard(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("discard: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		h.err = fmt.Errorf("discard: %w", dhl.ErrNoTx)
		return h.err
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.Exec(h.ctx, `ROLLBACK TO SAVEPOINT sp_`+name+`;`)
		if h.err != nil {
			h.err = fmt.Errorf("discard: %w", h.err)
			return h.err
		}
	}
	return nil
}

// Save a savepoint
func (h *PostgreSQLHelper) Save(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("save: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		h.err = fmt.Errorf("save: %w", dhl.ErrNoTx)
		return h.err
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.Exec(h.ctx, `RELEASE SAVEPOINT sp_`+name+`;`)
		if h.err != nil {
			h.err = fmt.Errorf("save: %w", h.err)
			return h.err
		}
	}
	return nil
}

// Query from PostgreSQL helper
func (h *PostgreSQLHelper) Query(query string, args ...any) (dhl.Rows, error) {
	if h.err != nil {
		return nil, h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("query: %w", dhl.ErrNoConn)
		return nil, h.err
	}

	var (
		sqr pgx.Rows
		placeholder,
		schema string
		paramInSeq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paramInSeq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	query = dhl.ReplaceQueryParamMarker(query, paramInSeq, placeholder)
	query = dhl.InterpolateTable(query, schema)
	if h.tx != nil {
		sqr, h.err = h.tx.Query(h.ctx, query, args...)
	} else {
		sqr, h.err = h.conn.Query(h.ctx, query, args...)
	}
	if h.err != nil {
		h.err = fmt.Errorf("query: %w", h.err)
		return nil, h.err
	}
	if sqr == nil {
		h.err = fmt.Errorf("query: %w", dhl.ErrNoConn)
		return nil, h.err
	}

	h.rws = NewPostgreSQLRows(&sqr)
	return h.rws, h.err
}

// QueryArray puts the single column result to an output array
func (h *PostgreSQLHelper) QueryArray(query string, out any, args ...any) error {
	var (
		sqr pgx.Rows
		placeholder,
		schema string
		paramInSeq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paramInSeq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	if h.err != nil {
		return h.err
	}
	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		return dhl.ErrArrayTypeNotSupported
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	query = dhl.ReplaceQueryParamMarker(query, paramInSeq, placeholder)
	// replace tables meant for interpolation {table} for putting the schema
	query = dhl.InterpolateTable(query, schema)
	if h.tx != nil {
		sqr, h.err = h.tx.Query(h.ctx, query, args...)
	} else {
		sqr, h.err = h.conn.Query(h.ctx, query, args...)
	}
	if h.err != nil {
		h.err = fmt.Errorf("queryarray: %w", h.err)
		return h.err
	}
	if sqr == nil {
		h.err = fmt.Errorf("queryarray: %w", dhl.ErrNoConn)
		return h.err
	}
	defer sqr.Close()

	switch t := out.(type) {
	case *[]string:
		idx := 0
		if t == nil {
			t = new([]string)
		}
		for sqr.Next() {
			*t = append(*t, "")
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int:
		idx := 0
		if t == nil {
			t = new([]int)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int8:
		idx := 0
		if t == nil {
			t = new([]int8)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int16:
		idx := 0
		if t == nil {
			t = new([]int16)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int32:
		idx := 0
		if t == nil {
			t = new([]int32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int64:
		idx := 0
		if t == nil {
			t = new([]int64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]bool:
		idx := 0
		if t == nil {
			t = new([]bool)
		}
		for sqr.Next() {
			*t = append(*t, false)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]float32:
		idx := 0
		if t == nil {
			t = new([]float32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]float64:
		idx := 0
		if t == nil {
			t = new([]float64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]time.Time:
		idx := 0
		if t == nil {
			t = new([]time.Time)
		}
		for sqr.Next() {
			*t = append(*t, time.Time{})
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	}
	return nil
}

// QueryRow from PostgreSQL helper
func (h *PostgreSQLHelper) QueryRow(query string, args ...any) dhl.Row {
	if h.err != nil {
		return nil
	}
	if h.conn == nil {
		h.err = fmt.Errorf("queryrow: %w", dhl.ErrNoConn)
		return nil
	}

	var (
		placeholder,
		schema string
		paramInSeq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paramInSeq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	query = dhl.ReplaceQueryParamMarker(query, paramInSeq, placeholder)
	query = dhl.InterpolateTable(query, schema)
	if h.tx != nil {
		return h.tx.QueryRow(h.ctx, query, args...)
	} else {
		return h.conn.QueryRow(h.ctx, query, args...)
	}
}

// Exec from PostgreSQL helper
func (h *PostgreSQLHelper) Exec(query string, args ...any) (int64, error) {
	if h.err != nil {
		return 0, h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("exec: %w", dhl.ErrNoConn)
		return 0, h.err
	}

	var (
		ct pgconn.CommandTag
		placeholder,
		schema string
		paramInSeq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paramInSeq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	query = dhl.ReplaceQueryParamMarker(query, paramInSeq, placeholder)
	query = dhl.InterpolateTable(query, schema)
	if h.tx != nil {
		ct, h.err = h.tx.Exec(h.ctx, query, args...)
		if h.err != nil {
			if !errors.Is(h.err, pgx.ErrTxClosed) {
				h.err = fmt.Errorf("exec: %w", h.err)
				return 0, h.err
			}
			h.err = nil
		}
		return ct.RowsAffected(), nil
	}

	ct, h.err = h.conn.Exec(h.ctx, query, args...)
	if h.err != nil {
		h.err = fmt.Errorf("exec: %w", h.err)
		return 0, h.err
	}

	return ct.RowsAffected(), nil
}

// Exists checks if a record exist
func (h *PostgreSQLHelper) Exists(queryWithParams string, args ...any) (bool, error) {

	var (
		sqlq,
		placeholder,
		schema string
		paramInSeq, exists bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paramInSeq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	queryWithParams = dhl.ReplaceQueryParamMarker(queryWithParams, paramInSeq, placeholder)
	queryWithParams = strings.TrimSpace(dhl.InterpolateTable(queryWithParams, schema))
	if strings.HasSuffix(queryWithParams, `;`) {
		return false, errors.New(`semicolons are not allowed at the end of this query`)
	}

	sqlq = `SELECT EXISTS (SELECT 1 FROM ` + queryWithParams + `);`
	if h.tx != nil {
		h.err = h.tx.QueryRow(h.ctx, sqlq, args...).Scan(&exists)
		if h.err != nil {
			if !errors.Is(h.err, dhl.ErrNoRows) {
				h.err = fmt.Errorf("exists: %w", h.err)
				return false, h.err
			}
			h.err = nil
		}
		return exists, h.err
	}

	h.err = h.conn.QueryRow(h.ctx, sqlq, args...).Scan(&exists)
	if h.err != nil {
		if !errors.Is(h.err, dhl.ErrNoRows) {
			h.err = fmt.Errorf("exists: %w", h.err)
			return false, h.err
		}
		h.err = nil
	}
	return exists, nil
}

// Next gets the next serial number
func (h *PostgreSQLHelper) Next(serial string, next *int64) error {

	var (
		sqlq, schema string
		affr         int64
	)
	if h.err != nil {
		return h.err
	}
	if next == nil {
		h.err = fmt.Errorf("next: %w", dhl.ErrVarMustBeInit)
		return h.err
	}
	if h.dbi.Schema != nil {
		schema = *h.dbi.Schema
	}
	// if the database config has set a sequence generator, this will use it
	sg := h.dbi.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			h.err = errors.New(`name place holder should be provided. ` +
				`Set name place holder in {placeholder} format. ` +
				`Place holder name should also be present in the upsert or select query`)
			return h.err
		}
		if sg.ResultQuery == "" {
			h.err = errors.New(`nesult query must be provided`)
			return h.err
		}

		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr = 1
		if sg.UpsertQuery != "" {
			sqlq = dhl.InterpolateTable(strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial), schema)
			if affr, h.err = h.Exec(sqlq); h.err != nil {
				h.err = fmt.Errorf("next: %w", h.err)
				return h.err
			}
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			h.err = errors.New(`upsert query did not insert or update any records`)
			return h.err
		}
		// result query needs a single scalar value to be returned
		sqlq = dhl.InterpolateTable(strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial), schema)
		if h.err = h.QueryRow(sqlq).Scan(next); h.err != nil {
			h.err = fmt.Errorf("next: %w", h.err)
			return h.err
		}
		return nil
	}

	// If there are no sequence configuration specified, we will create a sequence.
	// The format of the sequence should be <schema>.<sequence name>.
	// Dots are not allowed in the sequence name, therefore it must be converted to
	// another character, for example an underscore. If there is a dot specified
	// in the serial, it would be parsed as the schema.
	sln := serial
	if idx := strings.Index(serial, "."); idx != -1 {
		schema = serial[:idx]
		sln = strings.ReplaceAll(serial[idx+1:], ".", "_")
	}
	sln = "seq_" + sln
	if schema == "" {
		schema = "public"
	}

	seq := fmt.Sprintf(`
		CREATE SEQUENCE IF NOT EXISTS %s.%s
			INCREMENT 1
			START 1
			MINVALUE 1
			MAXVALUE 2147483647
			CACHE 1;`, schema, sln)

	// Check if sequence exists, if not create it
	// Get next value of the sequence
	sqlq = fmt.Sprintf("SELECT nextval('%s');", h.Escape(schema+"."+sln))
	if h.tx != nil {
		_, h.err = h.tx.Exec(h.ctx, seq)
		if h.err != nil {
			h.err = fmt.Errorf("next: %w", h.err)
			return h.err
		}
		h.err = h.tx.QueryRow(h.ctx, sqlq).Scan(next)
		if h.err != nil {
			h.err = fmt.Errorf("next: %w", h.err)
			return h.err
		}
		return nil
	}
	_, h.err = h.conn.Exec(h.ctx, seq)
	if h.err != nil {
		h.err = fmt.Errorf("next: %w", h.err)
		return h.err
	}
	h.err = h.conn.QueryRow(h.ctx, sqlq).Scan(next)
	if h.err != nil {
		h.err = fmt.Errorf("next: %w", h.err)
		return h.err
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *PostgreSQLHelper) VerifyWithin(tableName string, values []dhl.VerifyExpression) (Valid bool, Error error) {

	if h.err != nil {
		return false, h.err
	}
	if h.conn == nil {
		return false, fmt.Errorf("verify: %w", dhl.ErrNoConn)
	}

	var (
		andstr, sqlq,
		placeholder,
		schema, ph string
		paramInSeq, exists bool
		i                  int
	)

	args := make([]any, 0)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paramInSeq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	tableNameWithParameters := tableName
	if len(values) > 0 {
		tableNameWithParameters += ` WHERE `
	}
	ph = placeholder
	for _, v := range values {
		if isInterfaceNil(v.Value) {
			v.Operator = " IS NULL"
			ph = ""
		} else {
			if v.Operator == "" {
				v.Operator = "="
			}
			if paramInSeq {
				ph = placeholder + strconv.Itoa(i+1)
			}
			args = append(args, v.Value)
			i++
		}
		tableNameWithParameters += andstr + v.Name + v.Operator + ph
		andstr = " AND "
	}

	tableNameWithParameters = strings.TrimSpace(tableNameWithParameters)
	if strings.HasSuffix(tableNameWithParameters, `;`) {
		tableNameWithParameters, _ = strings.CutSuffix(tableNameWithParameters, `;`)
	}
	sqlq = dhl.InterpolateTable(`SELECT EXISTS (SELECT 1 FROM `+tableNameWithParameters+`);`, schema)
	h.err = h.QueryRow(sqlq, args...).Scan(&exists)
	if h.err != nil {
		if !errors.Is(h.err, dhl.ErrNoRows) {
			h.err = fmt.Errorf("verifywithin: %w", h.err)
			return false, h.err
		}
		h.err = nil
	}

	return exists, nil
}

// Escape a field value (fv) from disruption by single quote
func (h *PostgreSQLHelper) Escape(fv string) string {
	if len(fv) == 0 {
		return ""
	}
	senc := `'`
	sesc := `\`
	if h.dbi.StringEnclosingChar != nil && *h.dbi.StringEnclosingChar != "" {
		senc = *h.dbi.StringEnclosingChar
	}
	if h.dbi.StringEscapeChar != nil && *h.dbi.StringEscapeChar != "" {
		sesc = *h.dbi.StringEscapeChar
	}
	return strings.ReplaceAll(fv, senc, sesc+sesc)
}

// DatabaseVersion returns database version
func (h *PostgreSQLHelper) DatabaseVersion() string {
	var (
		version string
	)
	h.err = h.QueryRow(`SELECT version();`).Scan(&version)
	if h.err != nil {
		version = h.err.Error()
		h.err = nil
	}
	return version
}

// Now gets the current server date
func (h *PostgreSQLHelper) Now() *time.Time {
	var tm time.Time
	h.err = h.QueryRow(`SELECT NOW();`).Scan(&tm)
	if h.err != nil {
		tm = time.Now()
		h.err = nil
		return &tm
	}
	return &tm
}

// NowUTC gets the current server date in UTC
func (h *PostgreSQLHelper) NowUTC() *time.Time {
	var tm time.Time
	h.err = h.QueryRow(`SELECT timezone('UTC',CURRENT_TIMESTAMP);`).Scan(&tm)
	if h.err != nil {
		tm = time.Now().UTC()
		h.err = nil
		return &tm
	}
	return &tm
}

// Ping sends data packets to check pool connection
func (h *PostgreSQLHelper) Ping() error {
	return h.conn.Ping(h.ctx)
}

// PoolSet indicates that the helper was set externally
func (h *PostgreSQLHelper) PoolSet() {
	h.poolAtInit = true
}

// PoolUnset set the pool to unset
func (h *PostgreSQLHelper) PoolUnset() {
	h.poolAtInit = false
}
