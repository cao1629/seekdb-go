package goseekdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectionQuery tests the collection.Query() interface for vector similarity search
func TestCollectionQuery(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_query_" + uuid.New().String()[:8]
	collection := createTestCollection(t, client, collectionName, 3)
	defer func() {
		ctx := context.Background()
		_ = client.DeleteCollection(ctx, collectionName)
	}()

	ctx := context.Background()

	// Insert test data
	testData := []struct {
		id       string
		document string
		vector   []float32
		metadata Metadata
	}{
		{uuid.New().String(), "This is a test document about machine learning", []float32{1.0, 2.0, 3.0}, Metadata{"category": "AI", "score": 95, "tag": "ml"}},
		{uuid.New().String(), "Python programming tutorial for beginners", []float32{2.0, 3.0, 4.0}, Metadata{"category": "Programming", "score": 88, "tag": "python"}},
		{uuid.New().String(), "Advanced machine learning algorithms", []float32{1.1, 2.1, 3.1}, Metadata{"category": "AI", "score": 92, "tag": "ml"}},
		{uuid.New().String(), "Data science with Python", []float32{2.1, 3.1, 4.1}, Metadata{"category": "Data Science", "score": 90, "tag": "python"}},
		{uuid.New().String(), "Introduction to neural networks", []float32{1.2, 2.2, 3.2}, Metadata{"category": "AI", "score": 85, "tag": "neural"}},
	}

	// Insert all test data
	ids := make([]string, len(testData))
	docs := make([]string, len(testData))
	embeddings := make([][]float32, len(testData))
	metadatas := make([]Metadata, len(testData))
	for i, data := range testData {
		ids[i] = data.id
		docs[i] = data.document
		embeddings[i] = data.vector
		metadatas[i] = data.metadata
	}

	err := collection.Add(ctx, ids, docs,
		WithEmbeddings(embeddings),
		WithMetadatas(metadatas),
	)
	require.NoError(t, err)

	t.Run("basic vector similarity query", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 3,
			WithQueryEmbeddings([][]float32{queryVector}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Len(t, results.IDs, 1)      // Single query returns single result set
		assert.Greater(t, len(results.IDs[0]), 0)
		assert.LessOrEqual(t, len(results.IDs[0]), 3)
	})

	t.Run("query with metadata filter", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhere(Filter{"category": "AI"}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs[0]), 0)
	})

	t.Run("query with metadata filter $gte", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhere(Filter{"score": Filter{"$gte": 90}}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs[0]), 0)
	})

	t.Run("query with document filter", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhereDocument(Filter{"$contains": "machine learning"}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs[0]), 0)
	})

	t.Run("query with document filter using regex", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhereDocument(Filter{"$regex": ".*machine.*"}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs[0]), 0)
	})

	t.Run("query with combined filters", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhere(Filter{
				"category": Filter{"$eq": "AI"},
				"score":    Filter{"$gte": 90},
			}),
			WithWhereDocument(Filter{"$contains": "machine"}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
	})

	t.Run("query with $in operator", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhere(Filter{"tag": Filter{"$in": []interface{}{"ml", "python"}}}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs[0]), 0)
	})

	t.Run("query with include parameter", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 3,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithInclude([]string{"documents", "metadatas"}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		if len(results.IDs[0]) > 0 {
			assert.Len(t, results.Documents, 1)
			assert.Len(t, results.Metadatas, 1)
			assert.Equal(t, len(results.IDs[0]), len(results.Documents[0]))
			assert.Equal(t, len(results.IDs[0]), len(results.Metadatas[0]))
		}
	})

	t.Run("query with multiple vectors", func(t *testing.T) {
		queryVectors := [][]float32{
			{1.0, 2.0, 3.0},
			{2.0, 3.0, 4.0},
		}
		results, err := collection.Query(ctx, nil, 2,
			WithQueryEmbeddings(queryVectors),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Len(t, results.IDs, 2) // Two queries should return two result sets
		for i := range results.IDs {
			assert.Greater(t, len(results.IDs[i]), 0)
		}
	})

	t.Run("query returns distances", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 3,
			WithQueryEmbeddings([][]float32{queryVector}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		if len(results.IDs[0]) > 0 {
			assert.Len(t, results.Distances, 1)
			assert.Equal(t, len(results.IDs[0]), len(results.Distances[0]))
			// Verify distances are non-negative
			for _, dist := range results.Distances[0] {
				assert.GreaterOrEqual(t, dist, float64(0))
			}
		}
	})

	t.Run("query with logical operators $or", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 5,
			WithQueryEmbeddings([][]float32{queryVector}),
			WithWhere(Filter{
				"$or": []interface{}{
					map[string]interface{}{"category": map[string]interface{}{"$eq": "AI"}},
					map[string]interface{}{"tag": map[string]interface{}{"$eq": "python"}},
				},
			}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs[0]), 0)
	})
}

// TestCollectionQueryEmpty tests querying an empty collection
func TestCollectionQueryEmpty(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_query_empty_" + uuid.New().String()[:8]
	collection := createTestCollection(t, client, collectionName, 3)
	defer func() {
		ctx := context.Background()
		_ = client.DeleteCollection(ctx, collectionName)
	}()

	ctx := context.Background()

	t.Run("query empty collection returns empty results", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.Query(ctx, nil, 3,
			WithQueryEmbeddings([][]float32{queryVector}),
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Len(t, results.IDs, 1)
		assert.Len(t, results.IDs[0], 0)
	})
}
