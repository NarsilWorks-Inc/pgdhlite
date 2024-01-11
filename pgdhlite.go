package pgdhlite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"
	"github.com/segmentio/ksuid"

	cfg "github.com/eaglebush/config"
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
	trnmap   map[string]string
	closemu  sync.RWMutex
}

func init() {
	dhl.SetHelper(`pgdhlite`, &PostgreSQLHelper{})
	dhl.SetErrNoRows(pgx.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *PostgreSQLHelper) NewHelper() dhl.DataHelperLite {
	return &PostgreSQLHelper{
		trnmap: make(map[string]string),
	}
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
	h.con.Close()
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

// BeginDR begins a transaction with a transaction id
// also stored in to a local map or list. It will
// be useful if used in a deferred rollback setup
func (h *PostgreSQLHelper) BeginDR() (string, error) {
	tranid := ksuid.New().String()
	h.trnmap[tranid] = `OK`
	return tranid, h.Begin()
}

// Commit a transaction. The tranid argument is supplied
// using the BeginDR() function.
func (h *PostgreSQLHelper) Commit(tranid ...string) error {

	// tranid is used to identify the current transaction
	// if the coding style used is deferring rollback
	// after Begin() is called, this would solve the
	// problem of rolled back transaction in reusablity mode

	// If the tranid is not found on the map, it will
	// not take any action
	if len(tranid) > 0 {
		if _, ok := h.trnmap[tranid[0]]; !ok {
			return nil
		}

		// the key is deleted after calling Commit
		defer delete(h.trnmap, tranid[0])
	}

	if h.trcnt > 1 {
		h.closemu.Lock()
		defer h.closemu.Unlock()

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
	h.closemu.Lock()
	defer h.closemu.Unlock()
	if h.trcnt > 0 {
		h.trcnt--
	}

	// if trancount is zero, we can set the tx to nil
	if h.trcnt == 0 {
		h.tx = nil
	}

	return nil
}

// Rollback a transaction. The tranid argument is supplied
// using the BeginDR() function.
func (h *PostgreSQLHelper) Rollback(tranid ...string) error {

	// tranid is used to identify the current transaction
	// if the coding style used is deferring rollback
	// after Begin() is called, this would solve the
	// problem of rolled back transaction in reusablity mode

	// If the tranid is not found on the map, it will
	// not take any action
	if len(tranid) > 0 {
		if _, ok := h.trnmap[tranid[0]]; !ok {
			return nil
		}
		// the tranid is deleted when Rollback() is called
		defer delete(h.trnmap, tranid[0])
	}

	if h.trcnt > 1 {
		h.closemu.Lock()
		defer h.closemu.Unlock()
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
	h.closemu.Lock()
	defer h.closemu.Unlock()
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
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
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
		sqr, err = h.con.Query(h.ctx, sql, args...)
	}
	if err != nil {
		return err
	}
	defer sqr.Close()

	if sqr != nil {
		switch t := out.(type) {
		case *[]string:
			arr := make([]string, 0)
			var a string
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]int:
			arr := make([]int, 0)
			var a int
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]int8:
			arr := make([]int8, 0)
			var a int8
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]int16:
			arr := make([]int16, 0)
			var a int16
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]int32:
			arr := make([]int32, 0)
			var a int32
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]int64:
			arr := make([]int64, 0)
			var a int64
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]bool:
			arr := make([]bool, 0)
			var a bool
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]float32:
			arr := make([]float32, 0)
			var a float32
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]float64:
			arr := make([]float64, 0)
			var a float64
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		case *[]time.Time:
			arr := make([]time.Time, 0)
			var a time.Time
			for sqr.Next() {
				if err = sqr.Scan(&a); err != nil {
					return err
				}
				arr = append(arr, a)
			}
			if err = sqr.Err(); err != nil {
				return err
			}
			*t = arr
			_ = t
		}
	}
	return nil
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

	err = h.con.QueryRow(h.ctx, sql, args...).Scan(&exists)
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

	seq := fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS %s.%s
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

	err = h.con.QueryRow(h.ctx, sql).Scan(next)
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

	var tm *time.Time

	err := h.QueryRow(`SELECT NOW();`).Scan(&tm)
	if err != nil {
		tn := time.Now()
		return &tn
	}

	return tm
}

// NowUTC gets the current server date in UTC
func (h *PostgreSQLHelper) NowUTC() *time.Time {

	var tm *time.Time

	err := h.QueryRow(`SELECT timezone('UTC',CURRENT_TIMESTAMP);`).Scan(&tm)
	if err != nil {
		tn := time.Now().UTC()
		return &tn
	}

	return tm
}
