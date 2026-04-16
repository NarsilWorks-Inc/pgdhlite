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

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQLHelper implements DataHelperLite
type PostgreSQLHelper struct {
	hndl       dhl.DataHelperHandle
	ctx        context.Context
	tx         *sql.Tx
	trCnt      uint16
	rw         sync.RWMutex
	finalizeMu sync.Mutex
	err        error
	rollbackTriggered,
	committed bool
	frames           []bool
	manualCnt        uint16 // manual-mode nesting
	vendorStatements []vendorStmt
}

type vendorStmt struct {
	Key   string
	Value string
}

func init() {
	dhl.SetHelper("pgdhlite", &PostgreSQLHelper{})
	dhl.SetErrNoRows(pgx.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *PostgreSQLHelper) NewHelper() dhl.DataHelperLite {
	return &PostgreSQLHelper{
		vendorStatements: []vendorStmt{},
	}
}

// Acquire sets all queries to a new context from pool.
func (dh *PostgreSQLHelper) Acquire(ctx context.Context, h dhl.DataHelperHandle) error {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	dh.ctx = ctx
	dh.hndl = h
	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (dh *PostgreSQLHelper) Begin() (err error) {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if dh.err != nil {
		return dh.err
	}
	if dh.hndl == nil {
		dh.err = fmt.Errorf("begin: %w", dhl.ErrHandleNotSet)
		return dh.err
	}
	db := dh.hndl.DB()
	if db == nil {
		dh.err = fmt.Errorf("begin: %w", dhl.ErrHandleDBNotSet)
		return dh.err
	}
	if dh.manualCnt > 0 {
		dh.err = errors.New("begin: cannot mix Begin() with BeginManually() in the same transaction")
		return dh.err
	}
	defer handlePanic(&err)
	if dh.tx == nil {
		dh.tx, err = db.BeginTx(dh.ctx, nil)
		if err != nil {
			dh.err = fmt.Errorf("begin: %w", err)
			return dh.err
		}
	}
	// Increment transaction count
	dh.trCnt++
	dh.committed = false         // ✅ Reset commit state
	dh.rollbackTriggered = false // ✅ Reset rollback state

	// Track this scope’s deferred rollback
	dh.frames = append(dh.frames, true) // armed

	return nil
}

// Begin a transaction to support deferred rollback.
func (dh *PostgreSQLHelper) BeginManually() (err error) {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if dh.err != nil {
		return dh.err
	}
	if dh.hndl == nil {
		dh.err = fmt.Errorf("begin-manually: %w", dhl.ErrHandleNotSet)
		return dh.err
	}
	db := dh.hndl.DB()
	if db == nil {
		dh.err = fmt.Errorf("begin-manually: %w", dhl.ErrHandleDBNotSet)
		return dh.err
	}
	if dh.trCnt > 0 || len(dh.frames) > 0 {
		dh.err = errors.New("begin-manually: cannot mix BeginManually() with Begin() in the same transaction")
		return dh.err
	}
	defer handlePanic(&err)
	if dh.tx == nil {
		dh.tx, err = db.BeginTx(dh.ctx, nil)
		if err != nil {
			dh.err = fmt.Errorf("begin-manually: %w", err)
			return dh.err
		}
	}

	// Increment transaction count
	dh.trCnt++
	dh.manualCnt++
	dh.committed = false         // Reset commit state
	dh.rollbackTriggered = false // Reset rollback state
	return nil
}

// Commit a transaction.
func (dh *PostgreSQLHelper) Commit() (err error) {
	dh.rw.RLock()
	tx, trCnt, committed, rb, herr, hndl, manualCnt := dh.tx, dh.trCnt, dh.committed, dh.rollbackTriggered, dh.err, dh.hndl, dh.manualCnt
	dh.rw.RUnlock()
	if tx == nil || trCnt == 0 || rb || committed {
		return nil
	}
	if herr != nil {
		return dh.Rollback()
	}

	// Manual mode
	if manualCnt > 0 {
		if hndl == nil {
			dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleNotSet))
			return dh.err
		}
		if db := hndl.DB(); db == nil {
			dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleDBNotSet))
			return dh.err
		}
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			dh.rw.Unlock()
			return nil
		}
		defer handlePanic(&err)

		// outermost manual: real commit
		dh.finalizeMu.Lock()
		err = tx.Commit()
		dh.finalizeMu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			dh.setDHErr(fmt.Errorf("commit: %w", err))
			return dh.err
		}

		dh.rw.Lock()
		dh.manualCnt = 0
		dh.tx = nil
		// Treat ErrTxDone as success (idempotent commit)
		if err == nil || errors.Is(err, sql.ErrTxDone) {
			dh.committed = true
			dh.err = nil
		} else {
			dh.committed = false
		}
		dh.rollbackTriggered = false
		dh.frames = nil
		dh.trCnt = 0
		dh.rw.Unlock()
		return nil
	}

	// Deferred mode
	// If the transaction is not the outermost transaction, reduce transaction count.
	if trCnt > 1 {
		dh.rw.Lock()
		if n := len(dh.frames); n > 0 {
			dh.frames[n-1] = false // DISARMED
		}
		dh.trCnt--
		dh.rw.Unlock()
		return nil
	}

	// Ensure DB, connection, and transaction are valid before committing
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if db := hndl.DB(); db == nil {
		dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}

	defer handlePanic(&err)
	// Serialize finalization
	// Commit the outermost transaction
	dh.finalizeMu.Lock()
	err = tx.Commit()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.setDHErr(fmt.Errorf("commit: %w", err))
		return dh.err
	}

	// Mark committed, set transaction to nil and set rollback flag to false
	dh.rw.Lock()
	dh.trCnt = 0
	if err == nil || errors.Is(err, sql.ErrTxDone) {
		dh.committed = true
		dh.err = nil
	}
	dh.tx = nil
	dh.rollbackTriggered = false
	dh.frames = nil
	dh.rw.Unlock()
	return nil
}

// Rollback a transaction.
func (dh *PostgreSQLHelper) Rollback() (err error) {
	dh.rw.RLock()
	tx, trCnt, manualCnt, committed, herr := dh.tx, dh.trCnt, dh.manualCnt, dh.committed, dh.err
	dh.rw.RUnlock()

	if tx == nil || committed {
		return nil
	}

	// Manual mode
	if manualCnt > 0 {
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			dh.rw.Unlock()
			return nil
		}
		// outermost manual
		return dh.rollbk()
	}

	// Deferred mode
	// If this scope was committed earlier, its defer no-ops
	dh.rw.Lock()
	if n := len(dh.frames); n > 0 && !dh.frames[n-1] {
		dh.frames = dh.frames[:n-1]
		dh.rw.Unlock()
		return nil
	}
	dh.rw.Unlock()

	if herr != nil {
		return dh.rollbk()
	}

	if trCnt > 1 {
		dh.rw.Lock()
		dh.rollbackTriggered = true
		if n := len(dh.frames); n > 0 {
			dh.frames = dh.frames[:n-1] // pop armed
		}
		if dh.trCnt > 0 {
			dh.trCnt--
		}
		dh.rw.Unlock()
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	return dh.rollbk()
}

func (dh *PostgreSQLHelper) rollbk() (err error) {
	dh.rw.RLock()
	tx, hndl := dh.tx, dh.hndl
	dh.rw.RUnlock()
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("rollbk: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if db := hndl.DB(); db == nil {
		dh.setDHErr(fmt.Errorf("rollbk: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}
	dh.rw.Lock()
	dh.rollbackTriggered = true // 🔧 Mark rollback occurred
	dh.rw.Unlock()

	defer handlePanic(&err)
	// serialize finalization
	dh.finalizeMu.Lock()
	err = tx.Rollback()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.setDHErr(fmt.Errorf("rollbk: %w", err))
		return dh.err
	}

	// Reset all transaction state after rollback
	dh.rw.Lock()
	defer dh.rw.Unlock()
	dh.tx = nil
	dh.trCnt = 0
	dh.err = nil
	dh.committed = false         // 🔧 Reset flags
	dh.rollbackTriggered = false // 🔧 Reset flags (rollback is done)
	dh.frames = nil              // NEW: clear frames
	return nil
}

// Mark a savepoint
func (dh *PostgreSQLHelper) Mark(name string) (err error) {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if tx == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if db := hndl.DB(); db == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}

	if trCnt > 0 {
		defer handlePanic(&err)

		_, err = tx.ExecContext(dh.ctx, `SAVEPOINT sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("mark: %w", err))
			return dh.err
		}
	}

	return nil
}

// Discard a savepoint
func (dh *PostgreSQLHelper) Discard(name string) (err error) {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}
	if tx == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if db := hndl.DB(); db == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}

	if trCnt > 0 {
		defer handlePanic(&err)

		_, err = tx.ExecContext(dh.ctx, `ROLLBACK TO SAVEPOINT sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("discard: %w", err))
			return dh.err
		}
	}
	return nil
}

// Save a savepoint
func (dh *PostgreSQLHelper) Save(name string) (err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}
	if tx == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if db := hndl.DB(); db == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}
	if dh.trCnt > 0 {
		defer handlePanic(&err)

		_, err = tx.ExecContext(dh.ctx, `RELEASE SAVEPOINT sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("save: %w", err))
			return dh.err
		}
	}
	return nil
}

// Query from PostgreSQL helper
func (dh *PostgreSQLHelper) Query(querySql string, args ...any) (rows dhl.Rows, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return nil, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("query: %w", dhl.ErrHandleNotSet))
		return nil, dh.err
	}
	db := hndl.DB()
	if db == nil {
		dh.setDHErr(fmt.Errorf("query: %w", dhl.ErrHandleDBNotSet))
		return nil, dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	defer handlePanic(&err)

	var sqr *sql.Rows
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = db.QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("query: %w", err))
		return nil, dh.err
	}

	return NewPostgreSQLRows(sqr), dh.err
}

// QueryArray puts the single column result to an output array
func (dh *PostgreSQLHelper) QueryArray(querySql string, out any, args ...any) (err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	db := hndl.DB()
	if db == nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()
	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrArrayTypeNotSupported))
		return dh.err
	}

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	defer handlePanic(&err)

	var sqr *sql.Rows
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = db.QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", err))
		return dh.err
	}
	defer sqr.Close()

	switch t := out.(type) {
	case *[]string:
		for sqr.Next() {
			var v string
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int:
		for sqr.Next() {
			var v int
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int8:
		for sqr.Next() {
			var v int8
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int16:
		for sqr.Next() {
			var v int16
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int32:
		for sqr.Next() {
			var v int32
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int64:
		for sqr.Next() {
			var v int64
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]bool:
		for sqr.Next() {
			var v bool
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]float32:
		for sqr.Next() {
			var v float32
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]float64:
		for sqr.Next() {
			var v float64
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
	case *[]time.Time:
		for sqr.Next() {
			var v time.Time
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	}
	return nil
}

// QueryRow from PostgreSQL helper
func (dh *PostgreSQLHelper) QueryRow(querySql string, args ...any) dhl.Row {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		return NewPostgreSQLRow(nil)
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("queryrow: %w", dhl.ErrHandleNotSet))
		return NewPostgreSQLRow(nil)
	}

	db := hndl.DB()
	if db == nil {
		dh.setDHErr(fmt.Errorf("queryrow: %w", dhl.ErrHandleDBNotSet))
		return NewPostgreSQLRow(nil)
	}
	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	defer handlePanic(nil)
	if tx != nil {
		return NewPostgreSQLRow(tx.QueryRowContext(dh.ctx, querySql, args...))
	} else {
		return NewPostgreSQLRow(db.QueryRowContext(dh.ctx, querySql, args...))
	}
}

// Exec from PostgreSQL helper
func (dh *PostgreSQLHelper) Exec(querySql string, args ...any) (ra int64, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return 0, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("exec: %w", dhl.ErrHandleNotSet))
		return 0, dh.err
	}

	db := hndl.DB()
	if db == nil {
		dh.setDHErr(fmt.Errorf("exec: %w", dhl.ErrHandleDBNotSet))
		return 0, dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	defer handlePanic(&err)

	var sqr sql.Result
	if tx != nil {
		sqr, err = tx.ExecContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = db.ExecContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("exec: %w", err))
		return 0, dh.err
	}
	ra, _ = sqr.RowsAffected()

	return ra, nil
}

// Exists checks if a record exist
func (dh *PostgreSQLHelper) Exists(sqlWithParams string, args ...any) (exists bool, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return false, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("exists: %w", dhl.ErrHandleNotSet))
		return false, dh.err
	}

	db := hndl.DB()
	if db == nil {
		dh.setDHErr(fmt.Errorf("exists: %w", dhl.ErrHandleDBNotSet))
		return false, dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, paramInSeq, placeholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, schema)
	sqlWithParams = strings.TrimSpace(sqlWithParams)
	if strings.HasSuffix(sqlWithParams, `;`) {
		dh.setDHErr(errors.New(`semicolons are not allowed at the end of this query`))
		return false, dh.err
	}

	var b strings.Builder
	b.Grow(len(sqlWithParams) + 100)
	b.WriteString("SELECT EXISTS (SELECT 1 FROM ")
	b.WriteString(sqlWithParams)
	b.WriteString(`);`)
	sqlq := b.String()

	defer handlePanic(&err)

	if tx != nil {
		err = tx.QueryRowContext(dh.ctx, sqlq, args...).Scan(&exists)
		if err != nil {
			if !errors.Is(err, dhl.ErrNoRows) {
				dh.setDHErr(fmt.Errorf("exists: %w", err))
				return false, dh.err
			}
		}
		return exists, nil
	}
	err = db.QueryRowContext(dh.ctx, sqlq, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.setDHErr(fmt.Errorf("exists: %w", err))
			return false, dh.err
		}
	}
	return exists, nil
}

// Next gets the next serial number
func (dh *PostgreSQLHelper) Next(serial string, next *int64) (err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	db := hndl.DB()
	if db == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrHandleDBNotSet))
		return dh.err
	}

	if next == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrVarMustBeInit))
		return dh.err
	}

	di := hndl.DI()
	if di == nil {
		dh.setDHErr(errors.New("next: database info not configured"))
		return dh.err
	}

	schema := "public"
	if di.Schema != nil && *di.Schema != "" {
		schema = *di.Schema
	}

	defer handlePanic(&err)

	// if the database config has set a sequence generator, this will use it
	sg := di.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			dh.setDHErr(
				errors.New(`next: name place holder should be provided. ` +
					`Set name place holder in {placeholder} format. ` +
					`Place holder name should also be present in the upsert or select query`))
			return dh.err
		}
		if sg.ResultQuery == "" {
			dh.setDHErr(errors.New(`next: result query must be provided`))
			return dh.err
		}

		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr := int64(1)
		if sg.UpsertQuery != "" {
			var sqr sql.Result
			sqlq := dhl.InterpolateTable(strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial), schema)
			if tx != nil {
				sqr, err = tx.ExecContext(dh.ctx, sqlq)
			} else {
				sqr, err = db.ExecContext(dh.ctx, sqlq)
			}
			if err != nil {
				dh.setDHErr(fmt.Errorf("next: %w", err))
				return dh.err
			}
			affr, _ = sqr.RowsAffected()
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			dh.setDHErr(errors.New(`next: upsert query did not insert or update any records`))
			return dh.err
		}
		// result query needs a single scalar value to be returned
		sqlq := dhl.InterpolateTable(strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial), schema)
		if tx != nil {
			err = tx.QueryRowContext(dh.ctx, sqlq).Scan(next)
		} else {
			err = db.QueryRowContext(dh.ctx, sqlq).Scan(next)
		}
		if err != nil {
			dh.setDHErr(fmt.Errorf("next: %w", err))
			return dh.err
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

	sqlq := fmt.Sprintf("SELECT nextval('%s.%s');", schema, sln)
	scan := func() error {
		if tx != nil {
			return tx.QueryRowContext(dh.ctx, sqlq).Scan(next)
		}
		return db.QueryRowContext(dh.ctx, sqlq).Scan(next)
	}
	if err = scan(); err == nil {
		return nil
	} else {
		var sqlErr *pgconn.PgError
		if errors.As(err, &sqlErr) && sqlErr.Code == "42P01" { // object not found
			ddl := fmt.Sprintf(
				`CREATE SEQUENCE IF NOT EXISTS %s.%s
					INCREMENT 1
					START 1
					MINVALUE 1
					MAXVALUE 2147483647
					CACHE 1;`,
				schema, sln,
			)
			if tx != nil {
				if _, err2 := tx.ExecContext(dh.ctx, ddl); err2 != nil {
					return fmt.Errorf("next: %w", err2)
				}
			} else {
				if _, err2 := db.ExecContext(dh.ctx, ddl); err2 != nil {
					return fmt.Errorf("next: %w", err2)
				}
			}
			return scan() // retry
		}
		return fmt.Errorf("next: %w", err) // some other error
	}
}

// ExistsExt a set of validation expression against the underlying database table
func (dh *PostgreSQLHelper) ExistsExt(tableName string, values []dhl.ColumnFilter) (exists bool, err error) {
	dh.rw.RLock()
	herr, hndl := dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return false, herr
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("existsext: %w", dhl.ErrHandleNotSet))
		return false, dh.err
	}
	if tableName == "" {
		dh.setDHErr(fmt.Errorf("existsext: %s", "table name not set"))
		return false, dh.err
	}

	if db := hndl.DB(); db == nil {
		dh.setDHErr(fmt.Errorf("existsext: %w", dhl.ErrHandleDBNotSet))
		return false, dh.err
	}

	var (
		andstr, sqlq,
		ph string
		i int
	)

	args := make([]any, 0)

	placeholder, paraminseq, schema := dh.getParamDataInfo()

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
			// If there is no operator, we default to "="
			if v.Operator == "" {
				v.Operator = "="
			}
			if paraminseq {
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
	err = dh.QueryRow(sqlq, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.rw.Lock()
			dh.setDHErr(fmt.Errorf("existsext: %w", err))
			dh.rw.Unlock()
			return false, dh.err
		}
		return false, nil
	}

	return exists, nil
}

// Escape a field value (fv) from disruption by single quote
func (h *PostgreSQLHelper) Escape(fv string) string {
	if fv == "" {
		return ""
	}
	return strings.ReplaceAll(fv, `'`, `\'`)
}

// DatabaseVersion returns database version
func (h *PostgreSQLHelper) DatabaseVersion() string {
	var version string
	err := h.QueryRow(`SELECT version();`).Scan(&version)
	if err != nil {
		return err.Error()
	}
	return version
}

// Now gets the current server date
func (h *PostgreSQLHelper) Now() *time.Time {
	var tm time.Time
	err := h.QueryRow(`SELECT NOW();`).Scan(&tm)
	if err != nil {
		tm = time.Now()
		return &tm
	}
	return &tm
}

// NowUTC gets the current server date in UTC
func (h *PostgreSQLHelper) NowUTC() *time.Time {
	var tm time.Time
	err := h.QueryRow(`SELECT timezone('UTC',CURRENT_TIMESTAMP);`).Scan(&tm)
	if err != nil {
		tm = time.Now().UTC()
		return &tm
	}
	return &tm
}

// Ping sends data packets to check pool connection
func (dh *PostgreSQLHelper) Ping() (err error) {
	dh.rw.RLock()
	hndl, ctx := dh.hndl, dh.ctx
	dh.rw.RUnlock()
	defer handlePanic(&err)

	if hndl == nil {
		err = fmt.Errorf("ping: %w", dhl.ErrHandleNotSet)
		dh.setDHErr(err)
		return dh.err
	}

	db := hndl.DB()
	if db == nil {
		err = fmt.Errorf("ping: %w", dhl.ErrHandleDBNotSet)
		dh.setDHErr(err)
		return dh.err
	}
	return db.PingContext(ctx)
}

// UpsertReturning inserts a row into the table.
// If a conflict occurs on the specified unique columns:
//
//   - If updateColumns is empty, the existing row is returned unchanged
//   - If updateColumns is provided, the existing row is updated using EXCLUDED values
//
// Parameters:
//   - insertColumns - columns in the INSERT. All NOT NULL columns without defaults must be included.
//   - uniqueColumns - columns defining the conflict target
//   - updateColumns -
//     1. empty or nil → do not modify existing row on conflict
//     2. non-empty → DO UPDATE SET col = EXCLUDED.col
//   - returnColumns - columns to return
//   - args - values for insertColumns, in order
//
// The method always returns the resulting row.
func (dh *PostgreSQLHelper) UpsertReturning(
	tableName string,
	insertColumns []string,
	uniqueColumns []string,
	updateColumns []string,
	returnColumns []string,
	args ...any,
) (dhl.Row, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return NewPostgreSQLRow(nil), herr
	}
	if hndl == nil {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %w", dhl.ErrHandleNotSet)
	}
	if tableName == "" {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %s", "table name not set")
	}
	if len(insertColumns) == 0 {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %s", "insert columns needs to be set")
	}
	if len(uniqueColumns) == 0 {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %s", "unique columns needs to be set")
	}
	if len(returnColumns) == 0 {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %s", "return columns needs to be set")
	}
	if len(insertColumns) != len(args) {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %s", "insert columns count and arguments mismatch")
	}
	if len(updateColumns) > 0 {
		for _, updCol := range updateColumns {
			found := false
			for _, insCol := range insertColumns {
				if strings.EqualFold(insCol, updCol) {
					found = true
					break
				}
			}
			if !found {
				return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %s", "update columns does not exist in insert columns")
			}
		}
	}
	db := hndl.DB()
	if db == nil {
		return NewPostgreSQLRow(nil), fmt.Errorf("upsertreturning: %w", dhl.ErrHandleDBNotSet)
	}

	// Build query
	cma := ""
	sql := "INSERT INTO " + tableName + " (" + strings.Join(insertColumns, ",")
	sql += ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(insertColumns)), ",") + ")\n"
	sql += "ON CONFLICT (" + strings.Join(uniqueColumns, ",") + ")\n"
	sql += "DO UPDATE SET "
	if len(updateColumns) == 0 {
		sql += insertColumns[0] + "=" + tableName + "." + insertColumns[0]
	} else {
		for _, updCol := range updateColumns {
			sql += cma + updCol + "=EXCLUDED." + updCol
			cma = ","
		}
	}
	sql += "\n"
	sql += "RETURNING "
	cma = ""
	for _, retCol := range returnColumns {
		sql += cma + retCol
		cma = ","
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, paramInSeq, placeholder)
	sql = dhl.InterpolateTable(sql, schema)

	defer handlePanic(nil)
	if tx != nil {
		return NewPostgreSQLRow(tx.QueryRowContext(dh.ctx, sql, args...)), nil
	} else {
		return NewPostgreSQLRow(db.QueryRowContext(dh.ctx, sql, args...)), nil
	}
}

// VendorStatement returns a vendor-specific statement or query when present. Returns an empty string if not present
func (dh *PostgreSQLHelper) VendorStatement(key string) string {
	return ""
}

// VendorStatements Lists the vendor-specific statements implemented in a helper
func (dh *PostgreSQLHelper) VendorStatements() []string {
	return []string{}
}

func (dh *PostgreSQLHelper) getParamDataInfo() (ph string, pis bool, sch string) {
	dh.rw.RLock()
	h := dh.hndl
	dh.rw.RUnlock()
	ph = "?"
	sch = "public"
	if h == nil || h.DI() == nil {
		return
	}
	if h.DI().ParameterPlaceHolder != nil && *h.DI().ParameterPlaceHolder != "" {
		ph = *h.DI().ParameterPlaceHolder
	}
	if h.DI().ParameterInSequence != nil {
		pis = *h.DI().ParameterInSequence
	}
	if h.DI().Schema != nil && *h.DI().Schema != "" {
		sch = *h.DI().Schema
	}
	return
}

func sanitizeName(s string) string {
	// replace non [A-Za-z0-9_] with _
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			b[i] = c
		} else {
			b[i] = '_'
		}
	}
	if len(b) == 0 {
		return "sp"
	}
	return string(b)
}

func (dh *PostgreSQLHelper) setDHErr(err error) {
	dh.rw.Lock()
	dh.err = err
	dh.rw.Unlock()
}
