package connection

import (
	"context"
	"database/sql"
)

// Connection is an interface for database connections.
// It abstracts away the differences between embedded and remote connections.
type Connection interface {
	// Connect establishes a connection to the database.
	Connect(ctx context.Context) error

	// Close closes the connection.
	Close() error

	// IsConnected returns true if the connection is active.
	IsConnected() bool

	// Execute executes a query and returns the result.
	Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// Query executes a query and returns rows.
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// QueryRow executes a query that returns at most one row.
	QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row

	// Begin starts a transaction.
	Begin(ctx context.Context) (Tx, error)

	// Mode returns the connection mode ("embedded" or "remote").
	Mode() string

	// RawConnection returns the underlying connection for advanced use.
	RawConnection() interface{}
}

// Tx represents a database transaction.
type Tx interface {
	// Commit commits the transaction.
	Commit() error

	// Rollback rolls back the transaction.
	Rollback() error

	// Execute executes a query within the transaction.
	Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// Query executes a query within the transaction.
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// QueryRow executes a query that returns at most one row within the transaction.
	QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row
}
