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

	dhl "github.com/NarsilWorks-Inc/datahelperlite"
	cfg "github.com/eaglebush/config"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// PostgreSQLHelper implements DataHelperLite
type PostgreSQLHelper struct {
	conn *pgxpool.Pool
	dbi  *cfg.DatabaseInfo
	ctx  context.Context
	tx   pgx.Tx
	rws  dhl.Rows
	trCnt,
	reuseCnt,
	txInstIdx uint8
	rw     sync.RWMutex
	txInst map[uint8]uint8
	err    error
}

func init() {
	dhl.SetHelper(`pgdhlite`, &PostgreSQLHelper{})
	dhl.SetErrNoRows(pgx.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *PostgreSQLHelper) NewHelper() dhl.DataHelperLite {
	return &PostgreSQLHelper{
		txInst:    make(map[uint8]uint8),
		txInstIdx: 0,
	}
}

// Open a new connection
func (h *PostgreSQLHelper) Open(ctx context.Context, di *cfg.DatabaseInfo) error {
	if h.conn != nil {
		h.rw.Lock()
		h.reuseCnt++
		h.rw.Unlock()
		return nil
	}

	h.err = nil
	h.txInst = make(map[uint8]uint8)
	h.txInstIdx = 0
	h.dbi = di
	if ctx == nil {
		ctx = context.Background()
	}
	h.ctx = ctx

	var cfg *pgxpool.Config
	cfg, h.err = pgxpool.ParseConfig(di.ConnectionString)
	if h.err != nil {
		h.err = fmt.Errorf("open: %w", h.err)
		return h.err
	}

	if di.MaxOpenConnection != nil {
		cfg.MaxConns = int32(*di.MaxIdleConnection)
	}
	if di.MaxConnectionLifetime != nil {
		cfg.MaxConnLifetime = time.Duration(*di.MaxConnectionLifetime)
	}
	if di.MaxConnectionIdleTime != nil {
		cfg.MaxConnIdleTime = time.Duration(*di.MaxConnectionIdleTime)
	}
	h.conn, h.err = pgxpool.ConnectConfig(ctx, cfg)
	if h.err != nil {
		h.err = fmt.Errorf("open: %w", h.err)
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
	h.trCnt++
	h.txInst[h.trCnt] = 1
	h.txInstIdx = h.trCnt
	h.rw.Unlock()
	return nil
}

// Commit a transaction.
func (h *PostgreSQLHelper) Commit() error {
	// Return early if any of the conditions are true
	if h.tx == nil || h.trCnt == 0 || h.txInstIdx == 0 || len(h.txInst) == 0 {
		return nil
	}

	// If there is an error, we give the control to rollback
	if h.err != nil {
		return h.Rollback()
	}

	h.rw.Lock()
	defer h.rw.Unlock()

	// Check if the current transaction instance is valid
	if flag := h.txInst[h.txInstIdx]; flag == 0 {
		h.txInstIdx-- // Move to the previous transaction instance
		return nil
	}

	// If the transaction is not the first transaction,
	// reduce the transaction count and set the current map index value
	// as processed
	if h.trCnt > 1 {
		h.trCnt--
		h.txInst[h.txInstIdx] = 0 // Mark the current transaction as processed
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
	if h.trCnt == 1 {
		if h.err = h.tx.Commit(h.ctx); h.err != nil && !errors.Is(h.err, sql.ErrTxDone) {
			h.err = fmt.Errorf("commit: %w", h.err)
			return h.err
		}
	}

	// Reset transaction state after a successful commit
	h.tx = nil
	h.trCnt = 0
	h.txInstIdx = 0
	h.txInst = make(map[uint8]uint8)

	return nil
}

// Rollback a transaction.
func (h *PostgreSQLHelper) Rollback() error {

	// Return early if any of the conditions are true
	if h.tx == nil || h.trCnt == 0 || h.txInstIdx == 0 || len(h.txInst) == 0 {
		return nil
	}

	if h.err != nil {
		return h.rollbk()
	}

	// Handle nested transactions
	// If the value of the map is zero, we move to the earlier transaction
	if flag := h.txInst[h.txInstIdx]; flag == 0 {
		h.txInstIdx--
		return nil
	}

	// If the transaction is not the first transaction,
	// reduce the transaction count and set the current map index value
	// as processed
	if h.trCnt > 1 {
		h.trCnt--
		h.txInst[h.txInstIdx] = 0 // Mark the current transaction as processed
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	// If the queries resulted an error, we also roll it back
	if h.trCnt == 1 {
		return h.rollbk()
	}

	return nil
}

func (h *PostgreSQLHelper) rollbk() error {

	// Ensure DB, connection, and transaction are valid before rolling back
	if h.conn == nil {
		h.err = fmt.Errorf("rollback: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		h.err = fmt.Errorf("rollback: %w", dhl.ErrNoTx)
		return h.err
	}

	// Perform rollback
	if h.err = h.tx.Rollback(h.ctx); h.err != nil && !errors.Is(h.err, sql.ErrTxDone) {
		h.err = fmt.Errorf("rollback: %w", h.err)
	}

	// Reset all transaction state after rollback
	h.rw.Lock()
	defer h.rw.Unlock()

	h.tx = nil
	h.trCnt = 0
	h.txInstIdx = 0
	h.err = nil
	h.txInst = make(map[uint8]uint8)
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
func (h *PostgreSQLHelper) Query(sql string, args ...interface{}) (dhl.Rows, error) {
	var (
		sqr pgx.Rows
	)
	if h.err != nil {
		return nil, h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("query: %w", dhl.ErrNoConn)
		return nil, h.err
	}
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)
	if h.tx != nil {
		sqr, h.err = h.tx.Query(h.ctx, sql, args...)
	} else {
		sqr, h.err = h.conn.Query(h.ctx, sql, args...)
	}
	if h.err != nil {
		h.err = fmt.Errorf("query: %w", h.err)
		return h.rws, h.err
	}
	if sqr == nil {
		h.err = fmt.Errorf("query: %w", dhl.ErrNoConn)
		return nil, h.err
	}

	h.rws = NewPostgreSQLRows(&sqr)
	return h.rws, h.err
}

// QueryArray puts the single column result to an output array
func (h *PostgreSQLHelper) QueryArray(sql string, out interface{}, args ...interface{}) error {
	var (
		sqr pgx.Rows
	)
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
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	// replace tables meant for interpolation {table} for putting the schema
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)
	if h.tx != nil {
		sqr, h.err = h.tx.Query(h.ctx, sql, args...)
	} else {
		sqr, h.err = h.conn.Query(h.ctx, sql, args...)
	}
	if h.err != nil {
		h.err = fmt.Errorf("queryarray: %w", h.err)
		return h.err
	}
	defer sqr.Close()

	if sqr != nil {
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
	}
	return nil
}

// QueryRow from PostgreSQL helper
func (h *PostgreSQLHelper) QueryRow(sql string, args ...interface{}) dhl.Row {
	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)
	if h.tx != nil {
		return h.tx.QueryRow(h.ctx, sql, args...)
	} else {
		return h.conn.QueryRow(h.ctx, sql, args...)
	}
}

// Exec from PostgreSQL helper
func (h *PostgreSQLHelper) Exec(sql string, args ...interface{}) (int64, error) {

	var (
		ct pgconn.CommandTag
	)

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)
	if h.tx != nil {
		ct, h.err = h.tx.Exec(h.ctx, sql, args...)
		if h.err != nil {
			if !errors.Is(h.err, pgx.ErrTxClosed) {
				h.err = fmt.Errorf("exec: %w", h.err)
				return 0, h.err
			}
		}
		return ct.RowsAffected(), nil
	}

	ct, h.err = h.conn.Exec(h.ctx, sql, args...)
	if h.err != nil {
		h.err = fmt.Errorf("exec: %w", h.err)
		return 0, h.err
	}

	return ct.RowsAffected(), nil
}

// Exists checks if a record exist
func (h *PostgreSQLHelper) Exists(sqlwparams string, args ...interface{}) (bool, error) {

	var (
		exists bool
		sql    string
	)

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlwparams = dhl.ReplaceQueryParamMarker(sqlwparams, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sqlwparams = strings.TrimSpace(dhl.InterpolateTable(sqlwparams, h.dbi.Schema))
	if strings.HasSuffix(sqlwparams, `;`) {
		return false, errors.New(`semicolons are not allowed at the end of this query`)
	}

	sql = `SELECT EXISTS (SELECT 1 FROM ` + sqlwparams + `);`
	if h.tx != nil {
		h.err = h.tx.QueryRow(h.ctx, sql, args...).Scan(&exists)
		if errors.Is(h.err, dhl.ErrNoRows) {
			h.err = nil
			return false, h.err
		}
		if h.err != nil {
			h.err = fmt.Errorf("exists: %w", h.err)
			return false, h.err
		}
		return exists, h.err
	}

	h.err = h.conn.QueryRow(h.ctx, sql, args...).Scan(&exists)
	if errors.Is(h.err, dhl.ErrNoRows) {
		h.err = nil
		return false, h.err
	}
	if h.err != nil {
		h.err = fmt.Errorf("exists: %w", h.err)
		return false, h.err
	}
	return exists, nil
}

// Next gets the next serial number
func (h *PostgreSQLHelper) Next(serial string, next *int64) error {

	var (
		sql  string
		affr int64
	)
	if next == nil {
		return dhl.ErrVarMustBeInit
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
			sql = strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial)
			if affr, h.err = h.Exec(sql); h.err != nil {
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
		sql = strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial)
		if h.err = h.QueryRow(sql).Scan(next); h.err != nil {
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
	sch := "public"
	sln := serial
	if idx := strings.Index(serial, "."); idx != -1 {
		sch = serial[:idx]
		sln = strings.ReplaceAll(serial[idx+1:], ".", "_")
	}

	seq := fmt.Sprintf(`
		CREATE SEQUENCE IF NOT EXISTS %s.%s
			INCREMENT 1
			START 1
			MINVALUE 1
			MAXVALUE 2147483647
			CACHE 1;`, sch, sln)

	// Check if sequence exists, if not create it
	// Get next value of the sequence
	sql = fmt.Sprintf("SELECT nextval('%s');", h.Escape(sch+"."+sln))
	if h.tx != nil {
		_, h.err = h.tx.Exec(h.ctx, seq)
		if h.err != nil {
			h.err = fmt.Errorf("next: %w", h.err)
			return h.err
		}
		h.err = h.tx.QueryRow(h.ctx, sql).Scan(next)
		if h.err != nil {
			h.err = fmt.Errorf("next: %w", h.err)
			return h.err
		}
		return nil
	}
	_, h.err = h.tx.Exec(h.ctx, seq)
	if h.err != nil {
		h.err = fmt.Errorf("next: %w", h.err)
		return h.err
	}
	h.err = h.conn.QueryRow(h.ctx, sql).Scan(next)
	if h.err != nil {
		h.err = fmt.Errorf("next: %w", h.err)
		return h.err
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *PostgreSQLHelper) VerifyWithin(tableName string, values []dhl.VerifyExpression) (Valid bool, Error error) {
	tableNameWithParameters := tableName

	args := make([]any, 0)
	i := 0
	andstr := ""
	placeholder := h.dbi.ParameterPlaceholder
	if len(values) > 0 {
		tableNameWithParameters += ` WHERE `
	}

	for _, v := range values {
		if isInterfaceNil(v.Value) {
			v.Operator = " IS NULL"
		} else {
			// If there is no operator, we default to "="
			if v.Operator == "" {
				v.Operator = "="
			}
			if h.dbi.ParameterInSequence {
				placeholder = h.dbi.ParameterPlaceholder + strconv.Itoa(i+1)
			}
			i++
		}
		tableNameWithParameters += andstr + v.Name + v.Operator + placeholder
		args = append(args, v.Value)
		andstr = " AND "
	}

	var (
		sql    string
		exists bool
	)

	tableNameWithParameters = strings.TrimSpace(tableNameWithParameters)
	if strings.HasSuffix(tableNameWithParameters, `;`) {
		return false, errors.New(`semicolons are not allowed at the end of this query`)
	}
	sql = dhl.InterpolateTable(`SELECT EXISTS (SELECT 1 FROM `+tableNameWithParameters+`);`, h.dbi.Schema)
	h.err = h.QueryRow(sql, args...).Scan(&exists)
	if h.err != nil {
		if !errors.Is(h.err, dhl.ErrNoRows) {
			h.err = fmt.Errorf("verifywithin: %w", h.err)
			return false, h.err
		}
		h.err = nil
		return false, h.err
	}

	return exists, nil
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
