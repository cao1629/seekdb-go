package connection

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// RemoteConnection implements Connection for remote SeekDB/OceanBase servers.
type RemoteConnection struct {
	host     string
	port     int
	user     string
	password string
	database string
	tenant   string
	db       *sql.DB
}

// NewRemoteConnection creates a new remote connection.
func NewRemoteConnection(host string, port int, user, password, database, tenant string) *RemoteConnection {
	return &RemoteConnection{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		database: database,
		tenant:   tenant,
	}
}

// Connect establishes a connection to the remote server.
func (r *RemoteConnection) Connect(ctx context.Context) error {
	if r.db != nil {
		return nil // Already connected
	}

	// Build DSN (Data Source Name)
	// Format: user@tenant:password@tcp(host:port)/database?params
	username := r.user
	if r.tenant != "" && r.tenant != "test" {
		username = fmt.Sprintf("%s@%s", r.user, r.tenant)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local",
		username, r.password, r.host, r.port, r.database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	r.db = db
	return nil
}

// Close closes the connection.
func (r *RemoteConnection) Close() error {
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	r.db = nil
	return err
}

// IsConnected returns true if connected.
func (r *RemoteConnection) IsConnected() bool {
	if r.db == nil {
		return false
	}
	if err := r.db.Ping(); err != nil {
		return false
	}
	return true
}

// Execute executes a query.
func (r *RemoteConnection) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if r.db == nil {
		return nil, fmt.Errorf("not connected")
	}
	return r.db.ExecContext(ctx, query, args...)
}

// Query executes a query and returns rows.
func (r *RemoteConnection) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if r.db == nil {
		return nil, fmt.Errorf("not connected")
	}
	return r.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row.
func (r *RemoteConnection) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if r.db == nil {
		return nil
	}
	return r.db.QueryRowContext(ctx, query, args...)
}

// Begin starts a transaction.
func (r *RemoteConnection) Begin(ctx context.Context) (Tx, error) {
	if r.db == nil {
		return nil, fmt.Errorf("not connected")
	}
	sqlTx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &remoteTx{tx: sqlTx}, nil
}

// Mode returns "remote".
func (r *RemoteConnection) Mode() string {
	return "remote"
}

// RawConnection returns the underlying *sql.DB.
func (r *RemoteConnection) RawConnection() interface{} {
	return r.db
}

// remoteTx implements Tx for remote connections.
type remoteTx struct {
	tx *sql.Tx
}

func (t *remoteTx) Commit() error {
	return t.tx.Commit()
}

func (t *remoteTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *remoteTx) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *remoteTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *remoteTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}
