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
	if !(h.conn == nil) {
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
		return fmt.Errorf("open: %w", h.err)
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
		return fmt.Errorf("open: %w", h.err)
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
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		h.tx, h.err = h.conn.BeginTx(h.ctx, pgx.TxOptions{})
		if h.err != nil {
			return fmt.Errorf("begin: %w", h.err)
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
	h.rw.Lock()
	defer h.rw.Unlock()

	// Check if the current transaction instance is valid
	flag, ok := h.txInst[h.txInstIdx]
	if !ok || flag == 0 {
		if ok {
			h.txInstIdx-- // Move to the previous transaction instance
		}
		return nil
	}

	// Handle nested transactions
	if h.trCnt > 1 {
		h.trCnt--
		h.txInst[h.txInstIdx] = 0 // Mark the current transaction as processed
		return nil
	}

	// Ensure DB, connection, and transaction are valid before committing
	if h.conn == nil {
		return fmt.Errorf("commit: %w", dhl.ErrNoConn)
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		return fmt.Errorf("commit: %w", dhl.ErrNoTx)
	}

	// Commit the outermost transaction
	if h.trCnt == 1 {
		if err := h.tx.Commit(h.ctx); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return fmt.Errorf("commit: %w", err)
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

	h.rw.Lock()
	defer h.rw.Unlock()

	// If there's an error or this is the outermost transaction, rollback
	if h.err != nil || h.trCnt == 1 {
		// Ensure DB, connection, and transaction are valid before rolling back
		if h.conn == nil {
			return fmt.Errorf("rollback: %w", dhl.ErrNoConn)
		}
		if h.tx == nil || h.tx.Conn().IsClosed() {
			return fmt.Errorf("rollback: %w", dhl.ErrNoTx)
		}

		// Perform rollback
		if err := h.tx.Rollback(h.ctx); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return fmt.Errorf("rollback: %w", err)
		}

		// Reset all transaction state after rollback
		h.tx = nil
		h.trCnt = 0
		h.txInstIdx = 0
		h.txInst = make(map[uint8]uint8)
		return nil
	}

	// Handle nested transactions
	flag, ok := h.txInst[h.txInstIdx]
	if !ok || flag == 0 {
		if ok {
			h.txInstIdx-- // Move to the previous transaction instance
		}
		return nil
	}

	// Deduct transaction count for nested transactions
	if h.trCnt > 1 {
		h.trCnt--
		h.txInst[h.txInstIdx] = 0 // Mark the current transaction as processed
		return nil
	}

	return nil
}

// Mark a savepoint
func (h *PostgreSQLHelper) Mark(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		return fmt.Errorf("mark: %w", dhl.ErrNoConn)
	}
	if h.tx == nil {
		return fmt.Errorf("rollback: %w", dhl.ErrNoTx)
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.Exec(h.ctx, `SAVEPOINT sp_`+name+`;`)
	}
	return fmt.Errorf("mark: %w", h.err)
}

// Discard a savepoint
func (h *PostgreSQLHelper) Discard(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		return dhl.ErrNoTx
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.Exec(h.ctx, `ROLLBACK TO SAVEPOINT sp_`+name+`;`)
	}
	return fmt.Errorf("discard: %w", h.err)
}

// Save a savepoint
func (h *PostgreSQLHelper) Save(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		return fmt.Errorf("save: %w", dhl.ErrNoConn)
	}
	if h.tx == nil || h.tx.Conn().IsClosed() {
		return fmt.Errorf("save: %w", dhl.ErrNoTx)
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.Exec(h.ctx, `RELEASE SAVEPOINT sp_`+name+`;`)
	}
	return fmt.Errorf("save: %w", h.err)
}

// Query from PostgreSQL helper
func (h *PostgreSQLHelper) Query(sql string, args ...interface{}) (dhl.Rows, error) {
	var (
		err error
		sqr pgx.Rows
	)
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)
	if h.tx != nil {
		sqr, err = h.tx.Query(h.ctx, sql, args...)
	} else {
		sqr, err = h.conn.Query(h.ctx, sql, args...)
	}
	if err != nil {
		return h.rws, err
	}
	if sqr != nil {
		h.rws = NewPostgreSQLRows(&sqr)
	}
	return h.rws, nil
}

// QueryArray puts the single column result to an output array
func (h *PostgreSQLHelper) QueryArray(sql string, out interface{}, args ...interface{}) error {
	var (
		err error
		sqr pgx.Rows
	)
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
		sqr, err = h.tx.Query(h.ctx, sql, args...)
	} else {
		sqr, err = h.conn.Query(h.ctx, sql, args...)
	}
	if err != nil {
		return err
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
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]int:
			idx := 0
			if t == nil {
				t = new([]int)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]int8:
			idx := 0
			if t == nil {
				t = new([]int8)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]int16:
			idx := 0
			if t == nil {
				t = new([]int16)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]int32:
			idx := 0
			if t == nil {
				t = new([]int32)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]int64:
			idx := 0
			if t == nil {
				t = new([]int64)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]bool:
			idx := 0
			if t == nil {
				t = new([]bool)
			}
			for sqr.Next() {
				*t = append(*t, false)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]float32:
			idx := 0
			if t == nil {
				t = new([]float32)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]float64:
			idx := 0
			if t == nil {
				t = new([]float64)
			}
			for sqr.Next() {
				*t = append(*t, 0)
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			_ = t
		case *[]time.Time:
			idx := 0
			if t == nil {
				t = new([]time.Time)
			}
			for sqr.Next() {
				*t = append(*t, time.Time{})
				if err = sqr.Scan(&(*t)[idx]); err != nil {
					return err
				}
				idx++
			}
			if err = sqr.Err(); err != nil {
				return err
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

	ct, err = h.conn.Exec(h.ctx, sql, args...)
	if err != nil {
		return 0, err
	}

	return ct.RowsAffected(), nil
}

// Exists checks if a record exist
func (h *PostgreSQLHelper) Exists(sqlwparams string, args ...interface{}) (bool, error) {

	var (
		err    error
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
		err = h.tx.QueryRow(h.ctx, sql, args...).Scan(&exists)
		if errors.Is(err, dhl.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	err = h.conn.QueryRow(h.ctx, sql, args...).Scan(&exists)
	if errors.Is(err, dhl.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Next gets the next serial number
func (h *PostgreSQLHelper) Next(serial string, next *int64) error {

	var (
		err  error
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
			return errors.New(`name place holder should be provided. ` +
				`Set name place holder in {placeholder} format. ` +
				`Place holder name should also be present in the upsert or select query`)
		}
		if sg.ResultQuery == "" {
			return errors.New(`nesult query must be provided`)
		}

		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr = 1
		if sg.UpsertQuery != "" {
			sql = strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial)
			if affr, err = h.Exec(sql); err != nil {
				return err
			}
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			return errors.New(`upsert query did not insert or update any records`)
		}
		// result query needs a single scalar value to be returned
		sql = strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial)
		if err = h.QueryRow(sql).Scan(next); err != nil {
			return err
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
		_, err = h.tx.Exec(h.ctx, seq)
		if err != nil {
			return err
		}
		err = h.tx.QueryRow(h.ctx, sql).Scan(next)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = h.tx.Exec(h.ctx, seq)
	if err != nil {
		return err
	}
	err = h.conn.QueryRow(h.ctx, sql).Scan(next)
	if err != nil {
		return err
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *PostgreSQLHelper) VerifyWithin(tableName string, values []dhl.VerifyExpression) (Valid bool, Error error) {
	tableNameWithParameters := tableName

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

	tableNameWithParameters = strings.TrimSpace(tableNameWithParameters)
	if strings.HasSuffix(tableNameWithParameters, `;`) {
		return false, errors.New(`semicolons are not allowed at the end of this query`)
	}
	sql = dhl.InterpolateTable(`SELECT EXISTS (SELECT 1 FROM `+tableNameWithParameters+`);`, h.dbi.Schema)
	err = h.QueryRow(sql, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			return false, err
		}
		return false, nil
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
		err     error
		version string
	)
	err = h.QueryRow(`SELECT version();`).Scan(&version)
	if err != nil {
		version = err.Error()
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
