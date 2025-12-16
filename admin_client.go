package goseekdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/ob-labs/seekdb-go/internal/connection"
)

// AdminClient provides database-level operations.
type AdminClient struct {
	conn   connection.Connection
	config *ClientConfig
}

// NewAdminClient creates a new admin client for database management.
func NewAdminClient(opts ...ClientOption) (*AdminClient, error) {
	config := DefaultClientConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Admin operations typically use information_schema
	if config.Database == "" {
		config.Database = "information_schema"
	}

	var conn connection.Connection

	if config.Host != "" {
		// Remote mode
		conn = connection.NewRemoteConnection(
			config.Host,
			config.Port,
			config.User,
			config.Password,
			config.Database,
			config.Tenant,
		)
	} else if config.Path != "" {
		// Embedded mode
		conn = connection.NewEmbeddedConnection(config.Path, config.Database)
	} else {
		return nil, fmt.Errorf("%w: must specify either host or path", ErrInvalidParameter)
	}

	admin := &AdminClient{
		conn:   conn,
		config: config,
	}

	// Auto-connect if enabled
	if config.AutoConnect {
		if err := admin.Connect(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
	}

	return admin, nil
}

// Connect establishes a connection to the database.
func (a *AdminClient) Connect(ctx context.Context) error {
	return a.conn.Connect(ctx)
}

// Close closes the connection.
func (a *AdminClient) Close() error {
	return a.conn.Close()
}

// IsConnected returns true if connected.
func (a *AdminClient) IsConnected() bool {
	return a.conn.IsConnected()
}

// CreateDatabase creates a new database.
func (a *AdminClient) CreateDatabase(ctx context.Context, name string, tenant ...string) (*Database, error) {
	tenantName := a.config.Tenant
	if len(tenant) > 0 {
		tenantName = tenant[0]
	}

	createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name)
	if _, err := a.conn.Execute(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return &Database{
		Name:   name,
		Tenant: tenantName,
	}, nil
}

// GetDatabase retrieves database information.
func (a *AdminClient) GetDatabase(ctx context.Context, name string, tenant ...string) (*Database, error) {
	tenantName := a.config.Tenant
	if len(tenant) > 0 {
		tenantName = tenant[0]
	}

	query := `
		SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME = ?
	`

	row := a.conn.QueryRow(ctx, query, name)
	var schemaName, charset, collation string
	if err := row.Scan(&schemaName, &charset, &collation); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrDatabaseNotFound, name)
	}

	return &Database{
		Name:      schemaName,
		Tenant:    tenantName,
		Charset:   charset,
		Collation: collation,
	}, nil
}

// DeleteDatabase deletes a database.
func (a *AdminClient) DeleteDatabase(ctx context.Context, name string, tenant ...string) error {
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)
	if _, err := a.conn.Execute(ctx, dropSQL); err != nil {
		return fmt.Errorf("failed to delete database: %w", err)
	}
	return nil
}

// ListDatabases lists all databases.
func (a *AdminClient) ListDatabases(ctx context.Context, limit, offset int, tenant ...string) ([]Database, error) {
	query := `
		SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
	`

	// Add limit and offset
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := a.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	tenantName := a.config.Tenant
	if len(tenant) > 0 {
		tenantName = tenant[0]
	}

	var databases []Database
	for rows.Next() {
		var name, charset, collation string
		if err := rows.Scan(&name, &charset, &collation); err != nil {
			return nil, err
		}

		databases = append(databases, Database{
			Name:      name,
			Tenant:    tenantName,
			Charset:   charset,
			Collation: collation,
		})
	}

	return databases, nil
}

// HasDatabase checks if a database exists.
func (a *AdminClient) HasDatabase(ctx context.Context, name string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME = ?
	`

	row := a.conn.QueryRow(ctx, query, name)
	var count int
	if err := row.Scan(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}

// RawConnection returns the underlying connection for advanced use.
func (a *AdminClient) RawConnection() interface{} {
	return a.conn.RawConnection()
}

// DatabaseOption is a functional option for database operations.
type DatabaseOption func(*DatabaseConfig)

// DatabaseConfig holds database configuration.
type DatabaseConfig struct {
	Charset   string
	Collation string
}

// WithCharset sets the database charset.
func WithCharset(charset string) DatabaseOption {
	return func(c *DatabaseConfig) {
		c.Charset = charset
	}
}

// WithCollation sets the database collation.
func WithCollation(collation string) DatabaseOption {
	return func(c *DatabaseConfig) {
		c.Collation = collation
	}
}

// CreateDatabaseWithOptions creates a database with custom options.
func (a *AdminClient) CreateDatabaseWithOptions(ctx context.Context, name string, opts ...DatabaseOption) (*Database, error) {
	config := &DatabaseConfig{
		Charset:   "utf8mb4",
		Collation: "utf8mb4_general_ci",
	}

	for _, opt := range opts {
		opt(config)
	}

	var createParts []string
	createParts = append(createParts, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name))

	if config.Charset != "" {
		createParts = append(createParts, fmt.Sprintf("CHARACTER SET %s", config.Charset))
	}
	if config.Collation != "" {
		createParts = append(createParts, fmt.Sprintf("COLLATE %s", config.Collation))
	}

	createSQL := strings.Join(createParts, " ")
	if _, err := a.conn.Execute(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return &Database{
		Name:      name,
		Tenant:    a.config.Tenant,
		Charset:   config.Charset,
		Collation: config.Collation,
	}, nil
}
