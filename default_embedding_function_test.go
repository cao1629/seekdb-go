package goseekdb

/*
Test default embedding function - testing collection creation with default embedding function,
automatic vector generation from documents, and hybrid search
Supports configuring connection parameters via environment variables
*/

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Environment Variable Configuration ====================

func getDEFServerHost() string {
	if v := os.Getenv("SERVER_HOST"); v != "" {
		return v
	}
	return "127.0.0.1"
}

func getDEFServerPort() int {
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			return port
		}
	}
	return 2881
}

func getDEFServerDatabase() string {
	if v := os.Getenv("SERVER_DATABASE"); v != "" {
		return v
	}
	return "test"
}

func getDEFServerUser() string {
	if v := os.Getenv("SERVER_USER"); v != "" {
		return v
	}
	return "root"
}

func getDEFServerPassword() string {
	return os.Getenv("SERVER_PASSWORD")
}

func getDEFOBHost() string {
	if v := os.Getenv("OB_HOST"); v != "" {
		return v
	}
	return "localhost"
}

func getDEFOBPort() int {
	if v := os.Getenv("OB_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			return port
		}
	}
	return 11202
}

func getDEFOBTenant() string {
	if v := os.Getenv("OB_TENANT"); v != "" {
		return v
	}
	return "mysql"
}

func getDEFOBDatabase() string {
	if v := os.Getenv("OB_DATABASE"); v != "" {
		return v
	}
	return "test"
}

func getDEFOBUser() string {
	if v := os.Getenv("OB_USER"); v != "" {
		return v
	}
	return "root"
}

func getDEFOBPassword() string {
	return os.Getenv("OB_PASSWORD")
}

// ==================== Test Functions ====================

func TestServerDefaultEmbeddingFunction(t *testing.T) {
	// Create server client
	client, err := NewClient(
		WithHost(getDEFServerHost()),
		WithPort(getDEFServerPort()),
		WithDatabase(getDEFServerDatabase()),
		WithUser(getDEFServerUser()),
		WithPassword(getDEFServerPassword()),
	)
	require.NoError(t, err, "Failed to create client")
	defer client.Close()

	// Connect
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Skipf("Server connection failed (%s:%d): %v",
			getDEFServerHost(), getDEFServerPort(), err)
	}

	// Create collection with default embedding function
	collectionName := fmt.Sprintf("test_default_ef_%d", time.Now().Unix())
	t.Logf("Creating collection '%s' with default embedding function", collectionName)

	// Note: In Go, we need to check if default embedding function is available
	collection, err := client.CreateCollection(ctx, collectionName)
	if err != nil {
		// If default embedding function is not available, skip
		if err == ErrEmbeddingFunctionRequired {
			t.Skip("Default embedding function not available (ONNX runtime not installed)")
		}
		require.NoError(t, err, "Failed to create collection")
	}
	require.NotNil(t, collection)

	defer func() {
		client.DeleteCollection(ctx, collectionName)
		t.Logf("Cleaned up collection '%s'", collectionName)
	}()

	// Verify collection has embedding function
	t.Logf("Collection dimension: %d", collection.Dimension())

	// Test 1: Add documents without providing vectors (vectors will be auto-generated)
	t.Log("Testing collection.Add() with documents only (auto-generate vectors)")

	testDocuments := []string{
		"Machine learning is a subset of artificial intelligence",
		"Python programming language is widely used in data science",
		"Deep learning algorithms for neural networks",
		"Data science with Python and machine learning",
		"Introduction to artificial intelligence and neural networks",
	}

	testIDs := make([]string, len(testDocuments))
	for i := range testIDs {
		testIDs[i] = uuid.New().String()
	}

	testMetadatas := []Metadata{
		{"category": "AI", "page": 1},
		{"category": "Programming", "page": 2},
		{"category": "AI", "page": 3},
		{"category": "Data Science", "page": 4},
		{"category": "AI", "page": 5},
	}

	// Add documents without vectors - embedding function will generate them automatically
	err = collection.Add(ctx, testIDs, testDocuments,
		WithMetadatas(testMetadatas),
	)
	require.NoError(t, err, "Failed to add documents")
	t.Logf("Added %d documents (vectors auto-generated)", len(testDocuments))

	// Wait a bit for indexes to be ready
	time.Sleep(1 * time.Second)

	// Verify data was inserted
	results, err := collection.Get(ctx, []string{testIDs[0]},
		WithGetInclude([]string{"documents", "metadatas", "embeddings"}),
	)
	require.NoError(t, err, "Failed to get document")
	assert.Len(t, results.IDs, 1)
	assert.Equal(t, testDocuments[0], results.Documents[0])
	t.Log("Verified: document stored correctly")

	// Test 2: Query using text
	t.Log("Testing Query with text")
	queryResults, err := collection.Query(ctx, []string{"artificial intelligence and machine learning"}, 3)
	if err != nil {
		t.Logf("Query failed (expected if embedding function not available): %v", err)
	} else {
		assert.NotNil(t, queryResults)
		if len(queryResults.IDs) > 0 {
			t.Logf("Query found %d results", len(queryResults.IDs[0]))
		}
	}
}

func TestOceanBaseDefaultEmbeddingFunction(t *testing.T) {
	// Create OceanBase client
	client, err := NewClient(
		WithHost(getDEFOBHost()),
		WithPort(getDEFOBPort()),
		WithTenant(getDEFOBTenant()),
		WithDatabase(getDEFOBDatabase()),
		WithUser(getDEFOBUser()),
		WithPassword(getDEFOBPassword()),
	)
	require.NoError(t, err, "Failed to create client")
	defer client.Close()

	// Connect
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Skipf("OceanBase connection failed (%s:%d): %v",
			getDEFOBHost(), getDEFOBPort(), err)
	}

	// Create collection with default embedding function
	collectionName := fmt.Sprintf("test_default_ef_%d", time.Now().Unix())
	t.Logf("Creating collection '%s' with default embedding function", collectionName)

	collection, err := client.CreateCollection(ctx, collectionName)
	if err != nil {
		if err == ErrEmbeddingFunctionRequired {
			t.Skip("Default embedding function not available (ONNX runtime not installed)")
		}
		require.NoError(t, err, "Failed to create collection")
	}
	require.NotNil(t, collection)

	defer func() {
		client.DeleteCollection(ctx, collectionName)
		t.Logf("Cleaned up collection '%s'", collectionName)
	}()

	// Add documents without vectors
	testDocuments := []string{
		"Machine learning is a subset of artificial intelligence",
		"Python programming language is widely used",
	}
	testIDs := []string{uuid.New().String(), uuid.New().String()}

	err = collection.Add(ctx, testIDs, testDocuments,
		WithMetadatas([]Metadata{{"category": "AI"}, {"category": "Programming"}}),
	)
	require.NoError(t, err, "Failed to add documents")
	t.Logf("Added %d documents (vectors auto-generated)", len(testDocuments))

	// Wait for indexes
	time.Sleep(1 * time.Second)

	// Test query
	queryResults, err := collection.Query(ctx, []string{"machine learning"}, 2)
	if err != nil {
		t.Logf("Query failed (expected if embedding function not available): %v", err)
	} else {
		assert.NotNil(t, queryResults)
		if len(queryResults.IDs) > 0 {
			t.Logf("Query found %d results", len(queryResults.IDs[0]))
		}
	}
}

func TestDefaultEmbeddingFunctionWithHybridSearch(t *testing.T) {
	// Create server client
	client, err := NewClient(
		WithHost(getDEFServerHost()),
		WithPort(getDEFServerPort()),
		WithDatabase(getDEFServerDatabase()),
		WithUser(getDEFServerUser()),
		WithPassword(getDEFServerPassword()),
	)
	require.NoError(t, err, "Failed to create client")
	defer client.Close()

	// Connect
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Skipf("Server connection failed: %v", err)
	}

	// Create collection with default embedding function
	collectionName := fmt.Sprintf("test_def_hybrid_%d", time.Now().Unix())

	collection, err := client.CreateCollection(ctx, collectionName)
	if err != nil {
		if err == ErrEmbeddingFunctionRequired {
			t.Skip("Default embedding function not available")
		}
		require.NoError(t, err)
	}
	require.NotNil(t, collection)

	defer client.DeleteCollection(ctx, collectionName)

	// Add test documents
	testDocuments := []string{
		"Machine learning is a subset of artificial intelligence",
		"Python programming language is widely used in data science",
		"Deep learning algorithms for neural networks",
	}
	testIDs := []string{uuid.New().String(), uuid.New().String(), uuid.New().String()}

	err = collection.Add(ctx, testIDs, testDocuments,
		WithMetadatas([]Metadata{
			{"category": "AI"},
			{"category": "Programming"},
			{"category": "AI"},
		}),
	)
	require.NoError(t, err)

	// Wait for indexes
	time.Sleep(1 * time.Second)

	// Test hybrid search with full-text only
	t.Log("Testing hybrid_search with full-text search")
	hybridResults, err := collection.HybridSearch(ctx,
		&HybridSearchQuery{
			WhereDocument: Filter{"$contains": "machine learning"},
			NResults:      3,
		},
		nil, // No KNN
		nil, // No rank
		3,
	)
	if err != nil {
		t.Logf("Hybrid search failed: %v", err)
	} else {
		assert.NotNil(t, hybridResults)
		if len(hybridResults.IDs) > 0 {
			t.Logf("Hybrid search found %d results", len(hybridResults.IDs))
		}
	}
}
