package goseekdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectionHybridSearch tests the collection.HybridSearch() interface
func TestCollectionHybridSearch(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_hybrid_" + uuid.New().String()[:8]
	collection := createTestCollection(t, client, collectionName, 3)
	defer func() {
		ctx := context.Background()
		_ = client.DeleteCollection(ctx, collectionName)
	}()

	ctx := context.Background()

	// Insert test data designed for hybrid search testing
	testData := []struct {
		id       string
		document string
		vector   []float32
		metadata Metadata
	}{
		{uuid.New().String(), "Machine learning is a subset of artificial intelligence", []float32{1.0, 2.0, 3.0}, Metadata{"category": "AI", "page": 1, "score": 95, "tag": "ml"}},
		{uuid.New().String(), "Python programming language is widely used in data science", []float32{2.0, 3.0, 4.0}, Metadata{"category": "Programming", "page": 2, "score": 88, "tag": "python"}},
		{uuid.New().String(), "Deep learning algorithms for neural networks", []float32{1.1, 2.1, 3.1}, Metadata{"category": "AI", "page": 3, "score": 92, "tag": "ml"}},
		{uuid.New().String(), "Data science with Python and machine learning", []float32{2.1, 3.1, 4.1}, Metadata{"category": "Data Science", "page": 4, "score": 90, "tag": "python"}},
		{uuid.New().String(), "Introduction to artificial intelligence and neural networks", []float32{1.2, 2.2, 3.2}, Metadata{"category": "AI", "page": 5, "score": 85, "tag": "neural"}},
		{uuid.New().String(), "Advanced machine learning techniques and algorithms", []float32{1.3, 2.3, 3.3}, Metadata{"category": "AI", "page": 6, "score": 93, "tag": "ml"}},
		{uuid.New().String(), "Python tutorial for beginners in programming", []float32{2.2, 3.2, 4.2}, Metadata{"category": "Programming", "page": 7, "score": 87, "tag": "python"}},
		{uuid.New().String(), "Natural language processing with machine learning", []float32{1.4, 2.4, 3.4}, Metadata{"category": "AI", "page": 8, "score": 91, "tag": "nlp"}},
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

	// Wait for indexes to be ready
	time.Sleep(1 * time.Second)

	t.Run("hybrid search with full-text only", func(t *testing.T) {
		results, err := collection.HybridSearch(ctx,
			&HybridSearchQuery{
				WhereDocument: Filter{"$contains": "machine learning"},
				NResults:      5,
			},
			nil, // No KNN
			nil, // No rank config
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("hybrid search with vector only", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil, // No query
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				NResults:        5,
			},
			nil, // No rank config
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)

		// Verify distances are non-negative
		for _, dist := range results.Distances {
			assert.GreaterOrEqual(t, dist, float64(0))
		}
	})

	t.Run("hybrid search combined with RRF ranking", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			&HybridSearchQuery{
				WhereDocument: Filter{"$contains": "machine learning"},
				NResults:      10,
			},
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				NResults:        10,
			},
			&HybridSearchRank{
				RRF: &RRFConfig{K: 60},
			},
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("hybrid search with metadata filter on query", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			&HybridSearchQuery{
				WhereDocument: Filter{"$contains": "machine"},
				Where: Filter{
					"$and": []interface{}{
						map[string]interface{}{"category": map[string]interface{}{"$eq": "AI"}},
						map[string]interface{}{"page": map[string]interface{}{"$gte": 1}},
						map[string]interface{}{"page": map[string]interface{}{"$lte": 5}},
					},
				},
				NResults: 10,
			},
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"$and": []interface{}{
						map[string]interface{}{"category": map[string]interface{}{"$eq": "AI"}},
						map[string]interface{}{"score": map[string]interface{}{"$gte": 90}},
					},
				},
				NResults: 10,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)

		// Verify metadata filters are applied
		for _, meta := range results.Metadatas {
			assert.Equal(t, "AI", meta["category"])
		}
	})

	t.Run("hybrid search with logical operators", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			&HybridSearchQuery{
				WhereDocument: Filter{
					"$and": []interface{}{
						map[string]interface{}{"$contains": "machine"},
						map[string]interface{}{"$contains": "learning"},
					},
				},
				Where: Filter{
					"$or": []interface{}{
						map[string]interface{}{"tag": map[string]interface{}{"$eq": "ml"}},
						map[string]interface{}{"tag": map[string]interface{}{"$eq": "python"}},
					},
				},
				NResults: 10,
			},
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"tag": Filter{"$in": []interface{}{"ml", "python"}},
				},
				NResults: 10,
			},
			&HybridSearchRank{
				RRF: &RRFConfig{},
			},
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)

		// Verify logical operators are applied
		for _, meta := range results.Metadatas {
			if tag, ok := meta["tag"].(string); ok {
				assert.Contains(t, []string{"ml", "python"}, tag)
			}
		}
	})

	t.Run("hybrid search with $gte filter produces range condition", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"score": Filter{"$gte": 90},
				},
				NResults: 10,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)

		// Verify all results have score >= 90
		for _, meta := range results.Metadatas {
			if score, ok := meta["score"].(float64); ok {
				assert.GreaterOrEqual(t, score, float64(90))
			}
		}
	})

	t.Run("hybrid search with range filter $lte", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"page": Filter{"$lte": 5},
				},
				NResults: 10,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
	})

	t.Run("hybrid search with $ne filter", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"category": Filter{"$ne": "Programming"},
				},
				NResults: 10,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)

		// Verify no results have category = "Programming"
		for _, meta := range results.Metadatas {
			if cat, ok := meta["category"].(string); ok {
				assert.NotEqual(t, "Programming", cat)
			}
		}
	})

	t.Run("hybrid search with $nin filter", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"tag": Filter{"$nin": []interface{}{"python", "neural"}},
				},
				NResults: 10,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)

		// Verify no results have excluded tags
		for _, meta := range results.Metadatas {
			if tag, ok := meta["tag"].(string); ok {
				assert.NotContains(t, []string{"python", "neural"}, tag)
			}
		}
	})
}

// TestHybridSearchFilterTypes tests that Filter types work correctly in hybrid search
func TestHybridSearchFilterTypes(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_hybrid_filter_" + uuid.New().String()[:8]
	collection := createTestCollection(t, client, collectionName, 3)
	defer func() {
		ctx := context.Background()
		_ = client.DeleteCollection(ctx, collectionName)
	}()

	ctx := context.Background()

	// Insert test data
	testID := uuid.New().String()
	err := collection.Add(ctx,
		[]string{testID},
		[]string{"Test document about machine learning"},
		WithEmbeddings([][]float32{{1.0, 2.0, 3.0}}),
		WithMetadatas([]Metadata{{"category": "AI", "score": 95}}),
	)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	t.Run("nested Filter type works with $gte", func(t *testing.T) {
		// This test verifies the fix for Filter type assertion
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"score": Filter{"$gte": 90}, // Nested Filter type
				},
				NResults: 5,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("map[string]interface{} works with $gte", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				Where: Filter{
					"score": map[string]interface{}{"$gte": 90}, // map[string]interface{} type
				},
				NResults: 5,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Greater(t, len(results.IDs), 0)
	})
}

// TestHybridSearchEmpty tests hybrid search on empty collection
func TestHybridSearchEmpty(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_hybrid_empty_" + uuid.New().String()[:8]
	collection := createTestCollection(t, client, collectionName, 3)
	defer func() {
		ctx := context.Background()
		_ = client.DeleteCollection(ctx, collectionName)
	}()

	ctx := context.Background()

	t.Run("hybrid search on empty collection returns empty", func(t *testing.T) {
		queryVector := []float32{1.0, 2.0, 3.0}
		results, err := collection.HybridSearch(ctx,
			nil,
			&HybridSearchKNN{
				QueryEmbeddings: [][]float32{queryVector},
				NResults:        5,
			},
			nil,
			5,
		)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Len(t, results.IDs, 0)
	})
}
