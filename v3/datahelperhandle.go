package pgdhlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	dn "github.com/eaglebush/datainfo"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// DataHelperHandle manages the handle to the database connection
//
// It manages the resident database connection for proper pooling.
// This struct implements DataHelperHandler interface.
type DataHelperHandle struct {
	db   *sql.DB
	dbi  *dn.DataInfo
	err  error
	pool *pgxpool.Pool
}

// Open connects to the database and initializes it
func (dh *DataHelperHandle) Open(di *dn.DataInfo) error {
	if di == nil {
		return fmt.Errorf("open: no data info set")
	}
	if di.ConnectionString == nil {
		return fmt.Errorf("open: no data connection string set")
	}
	var cfg *pgxpool.Config
	cfg, dh.err = pgxpool.ParseConfig(*di.ConnectionString)
	if dh.err != nil {
		dh.err = fmt.Errorf("open: %w", dh.err)
		return dh.err
	}
	if di.MaxOpenConnection != nil {
		cfg.MaxConns = int32(*di.MaxOpenConnection)
	}
	if di.MaxIdleConnection != nil {
		cfg.MinIdleConns = int32(*di.MaxIdleConnection)
	}
	if di.MaxConnectionLifetime != nil {
		cfg.MaxConnLifetime = time.Duration(*di.MaxConnectionLifetime)
	}
	if di.MaxConnectionIdleTime != nil {
		cfg.MaxConnIdleTime = time.Duration(*di.MaxConnectionIdleTime)
	}
	dh.pool, dh.err = pgxpool.NewWithConfig(context.Background(), cfg)
	if dh.err != nil {
		dh.err = fmt.Errorf("open: %w", dh.err)
		return dh.err
	}
	if dh.pool == nil {
		dh.err = fmt.Errorf("open: failed to create pool")
		return dh.err
	}
	dh.db = stdlib.OpenDBFromPool(dh.pool)
	dh.dbi = di
	if err := dh.db.PingContext(context.Background()); err != nil {
		dh.err = fmt.Errorf("open: %w", err)
		return dh.err
	}
	return nil
}

// Ping tests the database connection
func (h *DataHelperHandle) Ping() error {
	if h.db == nil {
		return fmt.Errorf("ping: %s to use", dhl.ErrHandleNoHandle)
	}
	if err := h.db.PingContext(context.Background()); err != nil {
		h.err = fmt.Errorf("ping: %w", err)
		return h.err
	}
	return nil
}

// DB returns the database handle
func (h *DataHelperHandle) DB() *sql.DB {
	return h.db
}

// DI returns the data info that configured the handle
func (h *DataHelperHandle) DI() *dn.DataInfo {
	return h.dbi
}

// Close the database connection
func (h *DataHelperHandle) Close() error {
	if h.db == nil {
		return fmt.Errorf("ping: %s to close", dhl.ErrHandleNoHandle)
	}
	if h.err = h.db.Close(); h.err != nil {
		return h.err
	}
	h.db = nil
	h.pool.Close()
	return nil
}

// Err returns the last error
func (h *DataHelperHandle) Err() error {
	return h.err
}
