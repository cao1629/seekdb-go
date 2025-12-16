package goseekdb

import (
	"context"

	"github.com/ob-labs/seekdb-go/embedding"
)

// Collection represents a collection of documents with embeddings.
// It delegates all operations to the underlying client.
type Collection struct {
	client        collectionOperations
	name          string
	dimension     int
	distance      DistanceMetric
	embeddingFunc embedding.EmbeddingFunc
}

// collectionOperations defines the interface for collection operations on the client.
// This is implemented by the Client type.
type collectionOperations interface {
	collectionAdd(ctx context.Context, collectionName string, ids []string, documents []string, opts *AddOptions, embFunc embedding.EmbeddingFunc) error
	collectionUpdate(ctx context.Context, collectionName string, ids []string, opts *UpdateOptions, embFunc embedding.EmbeddingFunc) error
	collectionUpsert(ctx context.Context, collectionName string, ids []string, documents []string, opts *AddOptions, embFunc embedding.EmbeddingFunc) error
	collectionDelete(ctx context.Context, collectionName string, ids []string, where Filter, whereDocument Filter) error
	collectionQuery(ctx context.Context, collectionName string, queryTexts []string, nResults int, opts *QueryOptions, embFunc embedding.EmbeddingFunc, distance DistanceMetric) (*QueryResult, error)
	collectionGet(ctx context.Context, collectionName string, ids []string, opts *GetOptions) (*GetResult, error)
	collectionCount(ctx context.Context, collectionName string) (int, error)
	collectionHybridSearch(ctx context.Context, collectionName string, query *HybridSearchQuery, knn *HybridSearchKNN, rank *HybridSearchRank, nResults int, embFunc embedding.EmbeddingFunc, distance DistanceMetric) (*HybridSearchResult, error)
}

// Name returns the collection name.
func (c *Collection) Name() string {
	return c.name
}

// Dimension returns the vector dimension for this collection.
func (c *Collection) Dimension() int {
	return c.dimension
}

// Distance returns the distance metric used by this collection.
func (c *Collection) Distance() DistanceMetric {
	return c.distance
}

// Add adds documents to the collection.
// If embeddings are not provided, they will be generated using the embedding function.
func (c *Collection) Add(ctx context.Context, ids []string, documents []string, opts ...AddOption) error {
	options := &AddOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return c.client.collectionAdd(ctx, c.name, ids, documents, options, c.embeddingFunc)
}

// Update updates existing documents in the collection.
func (c *Collection) Update(ctx context.Context, ids []string, opts ...UpdateOption) error {
	options := &UpdateOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return c.client.collectionUpdate(ctx, c.name, ids, options, c.embeddingFunc)
}

// Upsert inserts or updates documents in the collection.
func (c *Collection) Upsert(ctx context.Context, ids []string, documents []string, opts ...AddOption) error {
	options := &AddOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return c.client.collectionUpsert(ctx, c.name, ids, documents, options, c.embeddingFunc)
}

// Delete deletes documents from the collection.
// You can delete by IDs, by filter, or both.
func (c *Collection) Delete(ctx context.Context, ids []string, where Filter, whereDocument Filter) error {
	return c.client.collectionDelete(ctx, c.name, ids, where, whereDocument)
}

// Query performs a vector similarity search.
// Either queryTexts or QueryEmbeddings (via WithQueryEmbeddings option) must be provided.
func (c *Collection) Query(ctx context.Context, queryTexts []string, nResults int, opts ...QueryOption) (*QueryResult, error) {
	options := &QueryOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return c.client.collectionQuery(ctx, c.name, queryTexts, nResults, options, c.embeddingFunc, c.distance)
}

// Get retrieves documents from the collection.
// You can filter by IDs, metadata filters, or document filters.
func (c *Collection) Get(ctx context.Context, ids []string, opts ...GetOption) (*GetResult, error) {
	options := &GetOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return c.client.collectionGet(ctx, c.name, ids, options)
}

// Count returns the number of documents in the collection.
func (c *Collection) Count(ctx context.Context) (int, error) {
	return c.client.collectionCount(ctx, c.name)
}

// HybridSearch performs a hybrid search combining full-text and vector search.
// Results are ranked using RRF (Reciprocal Rank Fusion).
func (c *Collection) HybridSearch(ctx context.Context, query *HybridSearchQuery, knn *HybridSearchKNN, rank *HybridSearchRank, nResults int) (*HybridSearchResult, error) {
	return c.client.collectionHybridSearch(ctx, c.name, query, knn, rank, nResults, c.embeddingFunc, c.distance)
}

// Peek returns the first few items from the collection without any filtering.
// This is useful for quickly inspecting the collection contents.
func (c *Collection) Peek(ctx context.Context, limit int) (*GetResult, error) {
	if limit <= 0 {
		limit = 10 // Default peek limit
	}
	return c.client.collectionGet(ctx, c.name, nil, &GetOptions{Limit: limit})
}
