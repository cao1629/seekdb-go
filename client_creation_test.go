package goseekdb

/*
Client creation and connection tests - testing connection and query execution for all three modes
Supports configuring connection parameters via environment variables
*/

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Environment Variable Configuration ====================

// Embedded mode
func getSeekDBPath() string {
	if v := os.Getenv("SEEKDB_PATH"); v != "" {
		return v
	}
	return "./seekdb_store"
}

func getSeekDBDatabase() string {
	if v := os.Getenv("SEEKDB_DATABASE"); v != "" {
		return v
	}
	return "test"
}

// Server mode
func getServerHost() string {
	if v := os.Getenv("SERVER_HOST"); v != "" {
		return v
	}
	return "127.0.0.1"
}

func getServerPort() int {
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			return port
		}
	}
	return 2881
}

func getServerDatabase() string {
	if v := os.Getenv("SERVER_DATABASE"); v != "" {
		return v
	}
	return "test"
}

func getServerUser() string {
	if v := os.Getenv("SERVER_USER"); v != "" {
		return v
	}
	return "root"
}

func getServerPassword() string {
	return os.Getenv("SERVER_PASSWORD")
}

// OceanBase mode
func getOBHost() string {
	if v := os.Getenv("OB_HOST"); v != "" {
		return v
	}
	return "localhost"
}

func getOBPort() int {
	if v := os.Getenv("OB_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			return port
		}
	}
	return 11202
}

func getOBTenant() string {
	if v := os.Getenv("OB_TENANT"); v != "" {
		return v
	}
	return "mysql"
}

func getOBDatabase() string {
	if v := os.Getenv("OB_DATABASE"); v != "" {
		return v
	}
	return "test"
}

func getOBUser() string {
	if v := os.Getenv("OB_USER"); v != "" {
		return v
	}
	return "root"
}

func getOBPassword() string {
	return os.Getenv("OB_PASSWORD")
}

// testCollectionManagement tests common collection management interfaces
func testCollectionManagement(t *testing.T, client *Client) {
	ctx := context.Background()

	// Test 1: CreateCollection - create a new collection
	testCollectionName := fmt.Sprintf("test_collection_%d", time.Now().Unix())
	testDimension := 128

	// Create collection with HNSW configuration
	config := &HNSWConfiguration{
		Dimension: testDimension,
		Distance:  DistanceCosine,
	}
	collection, err := client.CreateCollection(ctx, testCollectionName,
		WithConfiguration(config),
		WithCollectionEmbeddingFunc(nil), // No embedding function - use explicit embeddings
	)
	require.NoError(t, err, "Failed to create collection")
	require.NotNil(t, collection, "Collection should not be nil")
	assert.Equal(t, testCollectionName, collection.Name())

	actualDimension := collection.Dimension()
	assert.Greater(t, actualDimension, 0, "Collection dimension should be positive")

	t.Logf("Created collection '%s' with dimension: %d", testCollectionName, actualDimension)

	// Verify table was created
	tableName := GetTableName(testCollectionName)
	rows, err := client.Query(ctx, fmt.Sprintf("DESCRIBE `%s`", tableName))
	require.NoError(t, err, "Failed to describe table")
	defer rows.Close()

	columnNames := []string{}
	for rows.Next() {
		var field, colType, null, key string
		var defaultVal, extra interface{}
		if err := rows.Scan(&field, &colType, &null, &key, &defaultVal, &extra); err != nil {
			t.Logf("Warning: could not scan row: %v", err)
			continue
		}
		columnNames = append(columnNames, field)
	}

	assert.Contains(t, columnNames, "_id")
	assert.Contains(t, columnNames, "document")
	assert.Contains(t, columnNames, "embedding")
	assert.Contains(t, columnNames, "metadata")
	t.Logf("Table columns: %v", columnNames)

	// Cleanup function
	defer func() {
		client.Execute(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	}()

	// Test 2: GetCollection - get the collection we just created
	retrievedCollection, err := client.GetCollection(ctx, testCollectionName,
		WithCollectionEmbeddingFunc(nil),
	)
	require.NoError(t, err, "Failed to get collection")
	require.NotNil(t, retrievedCollection)
	assert.Equal(t, testCollectionName, retrievedCollection.Name())
	assert.Equal(t, actualDimension, retrievedCollection.Dimension())
	t.Logf("Retrieved collection '%s' with dimension: %d", retrievedCollection.Name(), retrievedCollection.Dimension())

	// Test 3: HasCollection - should return false for non-existent collection
	nonExistentName := fmt.Sprintf("test_collection_nonexistent_%d", time.Now().Unix())
	exists, err := client.HasCollection(ctx, nonExistentName)
	require.NoError(t, err)
	assert.False(t, exists, "HasCollection should return false for non-existent collection")
	t.Log("HasCollection correctly returns false for non-existent collection")

	// Test 4: HasCollection - should return true for existing collection
	exists, err = client.HasCollection(ctx, testCollectionName)
	require.NoError(t, err)
	assert.True(t, exists, "HasCollection should return true for existing collection")
	t.Log("HasCollection correctly returns true for existing collection")

	// Test 5: GetOrCreateCollection - should get existing collection
	existingCollection, err := client.CreateCollection(ctx, testCollectionName,
		WithConfiguration(config),
		WithCollectionEmbeddingFunc(nil),
		WithGetOrCreate(true),
	)
	require.NoError(t, err)
	require.NotNil(t, existingCollection)
	assert.Equal(t, testCollectionName, existingCollection.Name())
	assert.Equal(t, actualDimension, existingCollection.Dimension())
	t.Log("GetOrCreateCollection successfully retrieved existing collection")

	// Test 6: GetOrCreateCollection - should create new collection
	testCollectionNameMgmt := fmt.Sprintf("test_collection_mgmt_%d", time.Now().Unix())
	newCollection, err := client.CreateCollection(ctx, testCollectionNameMgmt,
		WithConfiguration(config),
		WithCollectionEmbeddingFunc(nil),
		WithGetOrCreate(true),
	)
	require.NoError(t, err)
	require.NotNil(t, newCollection)
	assert.Equal(t, testCollectionNameMgmt, newCollection.Name())
	t.Logf("GetOrCreateCollection successfully created collection '%s'", testCollectionNameMgmt)

	// Cleanup the second collection
	defer func() {
		client.DeleteCollection(ctx, testCollectionNameMgmt)
	}()

	// Test 7: ListCollections - should include our collections
	collections, err := client.ListCollections(ctx)
	require.NoError(t, err)
	assert.IsType(t, []CollectionInfo{}, collections)

	collectionNames := []string{}
	for _, c := range collections {
		collectionNames = append(collectionNames, c.Name)
	}
	assert.Contains(t, collectionNames, testCollectionName)
	assert.Contains(t, collectionNames, testCollectionNameMgmt)
	t.Logf("ListCollections successfully listed %d collections: %v", len(collections), collectionNames)

	// Test 8: DeleteCollection - should delete the collection
	err = client.DeleteCollection(ctx, testCollectionNameMgmt)
	require.NoError(t, err)

	exists, err = client.HasCollection(ctx, testCollectionNameMgmt)
	require.NoError(t, err)
	assert.False(t, exists, "Collection should be deleted")
	t.Logf("DeleteCollection successfully deleted collection '%s'", testCollectionNameMgmt)

	// Test 9: CountCollections - count the number of collections
	collectionCount, err := client.CountCollections(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, collectionCount, 1, "At least the test collection should exist")
	t.Logf("CountCollections returned count: %d", collectionCount)

	// Test 10: collection.Count() - count items in collection (should be 0 for empty collection)
	itemCount, err := collection.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, itemCount, "Empty collection should have 0 items")
	t.Logf("collection.Count() returned count: %d", itemCount)

	// Test 11: collection.Peek() - preview items in empty collection
	preview, err := collection.Peek(ctx, 5)
	require.NoError(t, err)
	require.NotNil(t, preview)
	assert.Len(t, preview.IDs, 0, "Empty collection should have no items")
	t.Logf("collection.Peek() returned %d items", len(preview.IDs))

	// Add some test data to test count and peek with data
	rng := rand.New(rand.NewSource(42)) // For reproducibility
	testIDs := []string{uuid.New().String(), uuid.New().String(), uuid.New().String()}
	embeddings := make([][]float32, 3)
	for i := range embeddings {
		embeddings[i] = make([]float32, collection.Dimension())
		for j := range embeddings[i] {
			embeddings[i][j] = rng.Float32()
		}
	}

	err = collection.Add(ctx, testIDs, []string{
		"Test document 0",
		"Test document 1",
		"Test document 2",
	},
		WithEmbeddings(embeddings),
		WithMetadatas([]Metadata{
			{"index": 0},
			{"index": 1},
			{"index": 2},
		}),
	)
	require.NoError(t, err, "Failed to add test data")

	// Test 12: collection.Count() - count items after adding data
	itemCountAfter, err := collection.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, itemCountAfter)
	t.Logf("collection.Count() after adding data: %d items", itemCountAfter)

	// Test 13: collection.Peek() - preview items with data
	previewWithData, err := collection.Peek(ctx, 2)
	require.NoError(t, err)
	require.NotNil(t, previewWithData)
	assert.Len(t, previewWithData.IDs, 2, "Should return limited items")
	assert.Len(t, previewWithData.Documents, 2)
	assert.Len(t, previewWithData.Metadatas, 2)
	assert.Len(t, previewWithData.Embeddings, 2)
	t.Logf("collection.Peek() with data returned %d items", len(previewWithData.IDs))

	// Test 14: collection.Peek() with different limit
	previewAll, err := collection.Peek(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, previewAll.IDs, 3, "Should return all 3 items")
	t.Logf("collection.Peek(limit=10) returned %d items", len(previewAll.IDs))
}

func TestCreateEmbeddedClient(t *testing.T) {
	t.Skip("Embedded client requires CGo and seekdb library - skipping for now")

	// Create client
	client, err := NewClient(
		WithPath(getSeekDBPath()),
		WithDatabase(getSeekDBDatabase()),
	)
	require.NoError(t, err, "Failed to create embedded client")
	defer client.Close()

	// Verify client type and properties
	assert.NotNil(t, client)
	assert.Equal(t, "embedded", client.Mode())
	assert.False(t, client.IsConnected(), "Should not be connected initially (lazy loading)")

	// Connect and verify
	ctx := context.Background()
	err = client.Connect(ctx)
	require.NoError(t, err, "Failed to connect")
	assert.True(t, client.IsConnected(), "Should be connected after Connect()")

	t.Logf("Embedded client created and connected successfully: path=%s, database=%s",
		getSeekDBPath(), getSeekDBDatabase())

	// Test all collection management interfaces
	testCollectionManagement(t, client)
}

func TestCreateServerClient(t *testing.T) {
	// Create client
	client, err := NewClient(
		WithHost(getServerHost()),
		WithPort(getServerPort()),
		WithTenant("sys"), // Default tenant for seekdb Server
		WithDatabase(getServerDatabase()),
		WithUser(getServerUser()),
		WithPassword(getServerPassword()),
	)
	require.NoError(t, err, "Failed to create server client")
	defer client.Close()

	// Verify client type and properties
	assert.NotNil(t, client)
	assert.Equal(t, "remote", client.Mode())

	// Connect
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Skipf("Server connection failed (%s:%d): %v\nHint: Please ensure seekdb Server is running on port %d",
			getServerHost(), getServerPort(), err, getServerPort())
	}

	assert.True(t, client.IsConnected(), "Should be connected after Connect()")

	// Execute query
	rows, err := client.Query(ctx, "SELECT 1 as test")
	require.NoError(t, err)
	defer rows.Close()

	assert.True(t, rows.Next(), "Should have at least one row")
	var testVal int
	err = rows.Scan(&testVal)
	require.NoError(t, err)
	assert.Equal(t, 1, testVal)

	t.Logf("Server client created and connected successfully: %s@%s:%d/%s",
		getServerUser(), getServerHost(), getServerPort(), getServerDatabase())

	// Test all collection management interfaces
	testCollectionManagement(t, client)
}

func TestCreateOceanBaseClient(t *testing.T) {
	// Create client
	client, err := NewClient(
		WithHost(getOBHost()),
		WithPort(getOBPort()),
		WithTenant(getOBTenant()),
		WithDatabase(getOBDatabase()),
		WithUser(getOBUser()),
		WithPassword(getOBPassword()),
	)
	require.NoError(t, err, "Failed to create OceanBase client")
	defer client.Close()

	// Verify client type and properties
	assert.NotNil(t, client)
	assert.Equal(t, "remote", client.Mode())
	assert.False(t, client.IsConnected(), "Should not be connected initially (lazy loading)")

	// Connect
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Skipf("OceanBase connection failed (%s:%d): %v\nHint: Please ensure OceanBase is running and tenant '%s' is created",
			getOBHost(), getOBPort(), err, getOBTenant())
	}

	assert.True(t, client.IsConnected(), "Should be connected after Connect()")

	// Execute query
	rows, err := client.Query(ctx, "SELECT 1 as test")
	require.NoError(t, err)
	defer rows.Close()

	assert.True(t, rows.Next(), "Should have at least one row")
	var testVal int
	err = rows.Scan(&testVal)
	require.NoError(t, err)
	assert.Equal(t, 1, testVal)

	t.Logf("OceanBase client created and connected successfully: %s@%s -> %s:%d/%s",
		getOBUser(), getOBTenant(), getOBHost(), getOBPort(), getOBDatabase())

	// Test all collection management interfaces
	testCollectionManagement(t, client)
}
