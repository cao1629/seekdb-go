package goseekdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectionGet tests the collection.Get() interface
func TestCollectionGet(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_get_" + uuid.New().String()[:8]
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

	t.Run("get by single ID", func(t *testing.T) {
		results, err := collection.Get(ctx, []string{testData[0].id})
		require.NoError(t, err)
		assert.Len(t, results.IDs, 1)
		assert.Equal(t, testData[0].id, results.IDs[0])
		assert.Equal(t, testData[0].document, results.Documents[0])
	})

	t.Run("get by multiple IDs", func(t *testing.T) {
		results, err := collection.Get(ctx, []string{testData[0].id, testData[1].id})
		require.NoError(t, err)
		assert.Len(t, results.IDs, 2)
	})

	t.Run("get with metadata filter $eq", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhere(Filter{"category": Filter{"$eq": "AI"}}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
		for _, meta := range results.Metadatas {
			assert.Equal(t, "AI", meta["category"])
		}
	})

	t.Run("get with metadata filter $gte", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhere(Filter{"score": Filter{"$gte": 90}}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
		for _, meta := range results.Metadatas {
			score, ok := meta["score"].(float64)
			if ok {
				assert.GreaterOrEqual(t, score, float64(90))
			}
		}
	})

	t.Run("get with metadata filter $in", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhere(Filter{"tag": Filter{"$in": []interface{}{"ml", "python"}}}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
		for _, meta := range results.Metadatas {
			tag := meta["tag"].(string)
			assert.Contains(t, []string{"ml", "python"}, tag)
		}
	})

	t.Run("get with document filter", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhereDocument(Filter{"$contains": "machine learning"}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("get with combined filters", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhere(Filter{"category": Filter{"$eq": "AI"}}),
			WithGetWhereDocument(Filter{"$contains": "machine"}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("get with limit and offset", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithLimit(2),
			WithOffset(1),
		)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results.IDs), 2)
	})

	t.Run("get with logical operators $or", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhere(Filter{
				"$or": []interface{}{
					map[string]interface{}{"category": "AI"},
					map[string]interface{}{"tag": "python"},
				},
			}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("get with logical operators $and", func(t *testing.T) {
		results, err := collection.Get(ctx, nil,
			WithGetWhere(Filter{
				"$and": []interface{}{
					map[string]interface{}{"category": map[string]interface{}{"$eq": "AI"}},
					map[string]interface{}{"score": map[string]interface{}{"$gte": 90}},
				},
			}),
			WithLimit(10),
		)
		require.NoError(t, err)
		assert.Greater(t, len(results.IDs), 0)
	})

	t.Run("get all with limit", func(t *testing.T) {
		results, err := collection.Get(ctx, nil, WithLimit(100))
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(results.IDs))
	})
}

// TestCollectionGetWithInclude tests the include parameter
func TestCollectionGetWithInclude(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_get_include_" + uuid.New().String()[:8]
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
		[]string{"Test document"},
		WithEmbeddings([][]float32{{1.0, 2.0, 3.0}}),
		WithMetadatas([]Metadata{{"key": "value"}}),
	)
	require.NoError(t, err)

	t.Run("get with include documents and metadatas", func(t *testing.T) {
		results, err := collection.Get(ctx, []string{testID},
			WithGetInclude([]string{"documents", "metadatas"}),
		)
		require.NoError(t, err)
		assert.Len(t, results.IDs, 1)
		assert.Len(t, results.Documents, 1)
		assert.Len(t, results.Metadatas, 1)
	})

	t.Run("get with include embeddings", func(t *testing.T) {
		results, err := collection.Get(ctx, []string{testID},
			WithGetInclude([]string{"documents", "metadatas", "embeddings"}),
		)
		require.NoError(t, err)
		assert.Len(t, results.IDs, 1)
		assert.Len(t, results.Embeddings, 1)
		assert.Len(t, results.Embeddings[0], 3)
	})
}

// TestCollectionGetNonExistent tests getting non-existent IDs
func TestCollectionGetNonExistent(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()

	collectionName := "test_get_nonexistent_" + uuid.New().String()[:8]
	collection := createTestCollection(t, client, collectionName, 3)
	defer func() {
		ctx := context.Background()
		_ = client.DeleteCollection(ctx, collectionName)
	}()

	ctx := context.Background()

	t.Run("get non-existent ID returns empty", func(t *testing.T) {
		results, err := collection.Get(ctx, []string{"non-existent-id"})
		require.NoError(t, err)
		assert.Len(t, results.IDs, 0)
	})
}
