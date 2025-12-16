package goseekdb

/*
AdminClient database management tests - testing all database CRUD operations
Tests create, get, list, and delete database operations for all three modes
Supports configuring connection parameters via environment variables
*/

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Environment Variable Configuration ====================

// Server mode (seekdb Server)
func getAdminServerHost() string {
	if v := os.Getenv("SERVER_HOST"); v != "" {
		return v
	}
	return "127.0.0.1"
}

func getAdminServerPort() int {
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			return port
		}
	}
	return 2881
}

func getAdminServerUser() string {
	if v := os.Getenv("SERVER_USER"); v != "" {
		return v
	}
	return "root"
}

func getAdminServerPassword() string {
	return os.Getenv("SERVER_PASSWORD")
}

// OceanBase mode
func getAdminOBHost() string {
	if v := os.Getenv("OB_HOST"); v != "" {
		return v
	}
	return "127.0.0.1"
}

func getAdminOBPort() int {
	if v := os.Getenv("OB_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			return port
		}
	}
	return 11202
}

func getAdminOBTenant() string {
	if v := os.Getenv("OB_TENANT"); v != "" {
		return v
	}
	return "mysql"
}

func getAdminOBUser() string {
	if v := os.Getenv("OB_USER"); v != "" {
		return v
	}
	return "root"
}

func getAdminOBPassword() string {
	return os.Getenv("OB_PASSWORD")
}

// testDatabaseOperations tests common database management operations
func testDatabaseOperations(t *testing.T, admin *AdminClient, expectedTenant string) {
	ctx := context.Background()
	testDBName := fmt.Sprintf("test_db_%d", testTimestamp())

	// Step 1: List all databases before test
	t.Log("Step 1: List all databases")
	databasesBefore, err := admin.ListDatabases(ctx, 0, 0)
	require.NoError(t, err, "Failed to list databases")
	require.NotNil(t, databasesBefore)
	t.Logf("Found %d databases before test", len(databasesBefore))

	// Step 2: Create new database
	t.Logf("Step 2: Create database '%s'", testDBName)
	db, err := admin.CreateDatabase(ctx, testDBName)
	require.NoError(t, err, "Failed to create database")
	require.NotNil(t, db)
	assert.Equal(t, testDBName, db.Name)
	t.Logf("Database '%s' created", testDBName)

	// Step 3: Get the created database and verify
	t.Logf("Step 3: Get database '%s' to verify creation", testDBName)
	retrievedDB, err := admin.GetDatabase(ctx, testDBName)
	require.NoError(t, err, "Failed to get database")
	require.NotNil(t, retrievedDB)
	assert.Equal(t, testDBName, retrievedDB.Name)
	if expectedTenant != "" {
		assert.Equal(t, expectedTenant, retrievedDB.Tenant)
	}
	t.Logf("Database retrieved: %s (tenant=%s, charset=%s, collation=%s)",
		retrievedDB.Name, retrievedDB.Tenant, retrievedDB.Charset, retrievedDB.Collation)

	// Step 4: HasDatabase - should return true for existing database
	exists, err := admin.HasDatabase(ctx, testDBName)
	require.NoError(t, err)
	assert.True(t, exists, "HasDatabase should return true for existing database")
	t.Log("HasDatabase correctly returns true for existing database")

	// Step 5: HasDatabase - should return false for non-existent database
	nonExistentDB := fmt.Sprintf("nonexistent_db_%d", testTimestamp())
	exists, err = admin.HasDatabase(ctx, nonExistentDB)
	require.NoError(t, err)
	assert.False(t, exists, "HasDatabase should return false for non-existent database")
	t.Log("HasDatabase correctly returns false for non-existent database")

	// Step 6: Delete the database
	t.Logf("Step 6: Delete database '%s'", testDBName)
	err = admin.DeleteDatabase(ctx, testDBName)
	require.NoError(t, err, "Failed to delete database")
	t.Logf("Database '%s' deleted", testDBName)

	// Step 7: List databases again to verify deletion
	t.Log("Step 7: List all databases to verify deletion")
	databasesAfter, err := admin.ListDatabases(ctx, 0, 0)
	require.NoError(t, err)
	t.Logf("Found %d databases after deletion", len(databasesAfter))

	// Verify the test database is not in the list
	dbNames := make([]string, len(databasesAfter))
	for i, db := range databasesAfter {
		dbNames[i] = db.Name
	}
	assert.NotContains(t, dbNames, testDBName, "Database '%s' should be deleted", testDBName)
	t.Logf("Verified: '%s' is not in the database list", testDBName)

	t.Log("All database management operations completed successfully!")
}

func TestEmbeddedAdminDatabaseOperations(t *testing.T) {
	t.Skip("Embedded admin client requires CGo and seekdb library - skipping for now")

	// Create admin client
	admin, err := NewAdminClient(
		WithPath("./seekdb_store"),
	)
	require.NoError(t, err, "Failed to create embedded admin client")
	defer admin.Close()

	// Connect
	ctx := context.Background()
	err = admin.Connect(ctx)
	require.NoError(t, err, "Failed to connect")

	t.Log("Embedded admin client created successfully")

	// Test database operations (embedded mode has no tenant)
	testDatabaseOperations(t, admin, "")
}

func TestServerAdminDatabaseOperations(t *testing.T) {
	// Create admin client
	admin, err := NewAdminClient(
		WithHost(getAdminServerHost()),
		WithPort(getAdminServerPort()),
		WithTenant("sys"), // Default tenant for seekdb Server
		WithUser(getAdminServerUser()),
		WithPassword(getAdminServerPassword()),
	)
	require.NoError(t, err, "Failed to create server admin client")
	defer admin.Close()

	// Connect
	ctx := context.Background()
	err = admin.Connect(ctx)
	if err != nil {
		t.Skipf("Server connection failed (%s:%d): %v\nHint: Please ensure seekdb Server is running on port %d",
			getAdminServerHost(), getAdminServerPort(), err, getAdminServerPort())
	}

	t.Logf("Server admin client created successfully: %s@%s:%d",
		getAdminServerUser(), getAdminServerHost(), getAdminServerPort())

	// Test database operations
	testDatabaseOperations(t, admin, "sys")
}

func TestOceanBaseAdminDatabaseOperations(t *testing.T) {
	// Create admin client
	admin, err := NewAdminClient(
		WithHost(getAdminOBHost()),
		WithPort(getAdminOBPort()),
		WithTenant(getAdminOBTenant()),
		WithUser(getAdminOBUser()),
		WithPassword(getAdminOBPassword()),
	)
	require.NoError(t, err, "Failed to create OceanBase admin client")
	defer admin.Close()

	// Connect
	ctx := context.Background()
	err = admin.Connect(ctx)
	if err != nil {
		t.Skipf("OceanBase connection failed (%s:%d): %v\nHint: Please ensure OceanBase is running and tenant '%s' is created",
			getAdminOBHost(), getAdminOBPort(), err, getAdminOBTenant())
	}

	t.Logf("OceanBase admin client created successfully: %s@%s@%s:%d",
		getAdminOBUser(), getAdminOBTenant(), getAdminOBHost(), getAdminOBPort())

	// Test database operations
	testDatabaseOperations(t, admin, getAdminOBTenant())
}

func TestCreateDatabaseWithOptions(t *testing.T) {
	// Create admin client
	admin, err := NewAdminClient(
		WithHost(getAdminServerHost()),
		WithPort(getAdminServerPort()),
		WithTenant("sys"),
		WithUser(getAdminServerUser()),
		WithPassword(getAdminServerPassword()),
	)
	require.NoError(t, err, "Failed to create admin client")
	defer admin.Close()

	// Connect
	ctx := context.Background()
	err = admin.Connect(ctx)
	if err != nil {
		t.Skipf("Server connection failed: %v", err)
	}

	testDBName := fmt.Sprintf("test_db_opts_%d", testTimestamp())

	// Create database with custom options
	db, err := admin.CreateDatabaseWithOptions(ctx, testDBName,
		WithCharset("utf8mb4"),
		WithCollation("utf8mb4_general_ci"),
	)
	require.NoError(t, err, "Failed to create database with options")
	require.NotNil(t, db)
	assert.Equal(t, testDBName, db.Name)
	assert.Equal(t, "utf8mb4", db.Charset)
	assert.Equal(t, "utf8mb4_general_ci", db.Collation)

	t.Logf("Created database '%s' with charset=%s, collation=%s",
		db.Name, db.Charset, db.Collation)

	// Cleanup
	err = admin.DeleteDatabase(ctx, testDBName)
	require.NoError(t, err, "Failed to delete database")
	t.Logf("Cleaned up database '%s'", testDBName)
}

// testTimestamp returns a Unix timestamp for unique naming
func testTimestamp() int64 {
	return int64(1000000000 + (testCounter * 1000))
}

var testCounter int

func init() {
	testCounter = int(^uint(0) >> 63) // Random starting point
}
