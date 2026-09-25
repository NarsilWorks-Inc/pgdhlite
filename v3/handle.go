package pgdhlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	dn "github.com/eaglebush/datainfo"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Handler manages the handle to the database connection
//
// It manages the resident database connection for proper pooling.
// This struct implements DataHelperHandler interface.
type Handle struct {
	db   *sql.DB
	dbi  *dn.DataInfo
	pool *pgxpool.Pool
	mu   sync.RWMutex
}

func init() {
	dhl.SetHandler("pgdhlite", &Handle{})
}

// Open connects to the database and initializes it
func (h *Handle) Open(di *dn.DataInfo) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.pool != nil || h.db != nil {
		return fmt.Errorf("open: database already open")
	}
	if di == nil {
		return fmt.Errorf("open: no data info set")
	}
	if di.ConnectionString == nil {
		return fmt.Errorf("open: no data connection string set")
	}

	var cfg *pgxpool.Config
	cfg, err = pgxpool.ParseConfig(*di.ConnectionString)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	// Set defaults
	cfg.MaxConns = 20
	cfg.MinConns = 4
	cfg.MinIdleConns = 4
	cfg.MaxConnIdleTime = 2 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 1 * time.Minute

	if di.MaxOpenConnection != nil {
		cfg.MaxConns = int32(*di.MaxOpenConnection)
	}

	// Minimum idle connection should be 20% if the maximum connections allowed
	minConns := int32(float64(cfg.MaxConns) * 0.20)
	if minConns < 1 {
		minConns = 1
	}

	cfg.MinConns = minConns
	cfg.MinIdleConns = minConns

	if di.MaxConnectionLifetime != nil {
		cfg.MaxConnLifetime = time.Duration(*di.MaxConnectionLifetime)
	}
	if di.MaxConnectionIdleTime != nil {
		cfg.MaxConnIdleTime = time.Duration(*di.MaxConnectionIdleTime)
	}

	// Added to handle sql.Open panic
	defer handlePanic(&err)

	var pool *pgxpool.Pool
	pool, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	if pool == nil {
		return fmt.Errorf("open: failed to create pool")
	}

	db := stdlib.OpenDBFromPool(pool)

	// Use a timeout for ping
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		// A failed ping should empty the db because this is the Open method
		_ = db.Close()
		pool.Close()

		return fmt.Errorf("open: %w", err)
	}

	h.pool = pool
	h.db = db
	h.dbi = di

	return nil
}

// Ping tests the database connection
func (h *Handle) Ping() (err error) {
	defer handlePanic(&err)

	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.db == nil {
		return fmt.Errorf("ping: %s to use", dhl.ErrHandleNoHandle)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err = h.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	return nil
}

// DB returns the database handle
func (h *Handle) DB() *sql.DB {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.db
}

// DI returns the data info that configured the handle
func (h *Handle) DI() *dn.DataInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.dbi
}

// Close the database connection
func (h *Handle) Close() (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db != nil {
		err = h.db.Close()
		h.db = nil
	}

	if h.pool != nil {
		h.pool.Close()
		h.pool = nil
	}

	if err != nil {
		return err
	}

	return nil
}

// Err returns the last error
// Note: This will be removed later in the interface
func (h *Handle) Err() error {
	return nil
}
