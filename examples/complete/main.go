package main

/*
Comprehensive Example: Complete guide to all goseekdb features

This example demonstrates all available operations:
1. Client connection (all modes)
2. Collection management
3. DML operations (add, update, upsert, delete)
4. DQL operations (query, get, hybrid_search)
5. Filter operators
6. Collection information methods

This is a complete reference for all client capabilities.
*/

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/google/uuid"
	"github.com/ob-labs/seekdb-go"
)

func main() {
	ctx := context.Background()

	// ============================================================================
	// PART 1: CLIENT CONNECTION
	// ============================================================================

	// Option 1: Embedded mode (local seekdb) - Not yet implemented
	// client, err := goseekdb.NewClient(
	//     goseekdb.WithPath("./seekdb"),
	//     goseekdb.WithDatabase("test"),
	// )

	// Option 2: Server mode (remote seekdb server)
	client, err := goseekdb.NewClient(
		goseekdb.WithHost("127.0.0.1"),
		goseekdb.WithPort(2881),
		goseekdb.WithDatabase("test"),
		goseekdb.WithUser("root"),
		goseekdb.WithPassword(""),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Option 3: Remote server mode (OceanBase Server)
	// client, err := goseekdb.NewClient(
	//     goseekdb.WithHost("127.0.0.1"),
	//     goseekdb.WithPort(2881),
	//     goseekdb.WithTenant("test"),
	//     goseekdb.WithDatabase("test"),
	//     goseekdb.WithUser("root"),
	//     goseekdb.WithPassword(""),
	// )

	// ============================================================================
	// PART 2: COLLECTION MANAGEMENT
	// ============================================================================

	collectionName := "comprehensive_example"
	dimension := 128

	// 2.1 Create a collection
	config := &goseekdb.HNSWConfiguration{
		Dimension: dimension,
		Distance:  goseekdb.DistanceCosine,
	}

	collection, err := client.CreateCollection(ctx, collectionName,
		goseekdb.WithConfiguration(config),
		goseekdb.WithCollectionEmbeddingFunc(nil), // Explicitly set to None since we're using custom embeddings
		goseekdb.WithGetOrCreate(true),
	)
	if err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}

	// 2.2 Check if collection exists
	exists, _ := client.HasCollection(ctx, collectionName)
	_ = exists

	// 2.3 Get collection object
	retrievedCollection, _ := client.GetCollection(ctx, collectionName,
		goseekdb.WithCollectionEmbeddingFunc(nil),
	)
	_ = retrievedCollection

	// 2.4 List all collections
	allCollections, _ := client.ListCollections(ctx)
	_ = allCollections

	// 2.5 Get or create collection (creates if doesn't exist)
	config2 := &goseekdb.HNSWConfiguration{
		Dimension: 64,
		Distance:  goseekdb.DistanceCosine,
	}
	collection2, _ := client.CreateCollection(ctx, "another_collection",
		goseekdb.WithConfiguration(config2),
		goseekdb.WithCollectionEmbeddingFunc(nil), // Explicitly set to None since we're using custom 64-dim embeddings
		goseekdb.WithGetOrCreate(true),
	)
	_ = collection2

	// ============================================================================
	// PART 3: DML OPERATIONS - ADD DATA
	// ============================================================================

	// Generate sample data
	rand.Seed(42)
	documents := []string{
		"Machine learning is transforming the way we solve problems",
		"Python programming language is widely used in data science",
		"Vector databases enable efficient similarity search",
		"Neural networks mimic the structure of the human brain",
		"Natural language processing helps computers understand human language",
		"Deep learning requires large amounts of training data",
		"Reinforcement learning agents learn through trial and error",
		"Computer vision enables machines to interpret visual information",
	}

	// Generate embeddings (in real usage, use an embedding model)
	embeddings := make([][]float32, len(documents))
	for i := range embeddings {
		embeddings[i] = make([]float32, dimension)
		for j := range embeddings[i] {
			embeddings[i][j] = rand.Float32()
		}
	}

	ids := make([]string, len(documents))
	for i := range ids {
		ids[i] = uuid.New().String()
	}

	// 3.1 Add single item
	singleID := uuid.New().String()
	singleEmbedding := make([]float32, dimension)
	for j := range singleEmbedding {
		singleEmbedding[j] = rand.Float32()
	}

	collection.Add(ctx,
		[]string{singleID},
		[]string{"This is a single document"},
		goseekdb.WithEmbeddings([][]float32{singleEmbedding}),
		goseekdb.WithMetadatas([]goseekdb.Metadata{
			{"type": "single", "category": "test"},
		}),
	)

	// 3.2 Add multiple items
	collection.Add(ctx, ids, documents,
		goseekdb.WithEmbeddings(embeddings),
		goseekdb.WithMetadatas([]goseekdb.Metadata{
			{"category": "AI", "score": 95, "tag": "ml", "year": 2023},
			{"category": "Programming", "score": 88, "tag": "python", "year": 2022},
			{"category": "Database", "score": 92, "tag": "vector", "year": 2023},
			{"category": "AI", "score": 90, "tag": "neural", "year": 2022},
			{"category": "NLP", "score": 87, "tag": "language", "year": 2023},
			{"category": "AI", "score": 93, "tag": "deep", "year": 2023},
			{"category": "AI", "score": 85, "tag": "reinforcement", "year": 2022},
			{"category": "CV", "score": 91, "tag": "vision", "year": 2023},
		}),
	)

	// 3.3 Add with only embeddings (no documents)
	vectorOnlyIDs := []string{uuid.New().String(), uuid.New().String()}
	vectorOnlyEmbeddings := make([][]float32, 2)
	for i := range vectorOnlyEmbeddings {
		vectorOnlyEmbeddings[i] = make([]float32, dimension)
		for j := range vectorOnlyEmbeddings[i] {
			vectorOnlyEmbeddings[i][j] = rand.Float32()
		}
	}

	collection.Add(ctx, vectorOnlyIDs, nil,
		goseekdb.WithEmbeddings(vectorOnlyEmbeddings),
		goseekdb.WithMetadatas([]goseekdb.Metadata{
			{"type": "vector_only"},
			{"type": "vector_only"},
		}),
	)

	// ============================================================================
	// PART 4: DML OPERATIONS - UPDATE DATA
	// ============================================================================

	// 4.1 Update single item
	collection.Update(ctx, []string{ids[0]},
		goseekdb.WithUpdateMetadatas([]goseekdb.Metadata{
			{"category": "AI", "score": 98, "tag": "ml", "year": 2024, "updated": true},
		}),
	)

	// 4.2 Update multiple items
	updateEmbeddings := make([][]float32, 2)
	for i := range updateEmbeddings {
		updateEmbeddings[i] = make([]float32, dimension)
		for j := range updateEmbeddings[i] {
			updateEmbeddings[i][j] = rand.Float32()
		}
	}

	collection.Update(ctx, ids[1:3],
		goseekdb.WithUpdateDocuments([]string{"Updated document 1", "Updated document 2"}),
		goseekdb.WithUpdateEmbeddings(updateEmbeddings),
		goseekdb.WithUpdateMetadatas([]goseekdb.Metadata{
			{"category": "Programming", "score": 95, "updated": true},
			{"category": "Database", "score": 97, "updated": true},
		}),
	)

	// 4.3 Update embeddings
	newEmbeddings := make([][]float32, 2)
	for i := range newEmbeddings {
		newEmbeddings[i] = make([]float32, dimension)
		for j := range newEmbeddings[i] {
			newEmbeddings[i][j] = rand.Float32()
		}
	}

	collection.Update(ctx, ids[2:4],
		goseekdb.WithUpdateEmbeddings(newEmbeddings),
	)

	// ============================================================================
	// PART 5: DML OPERATIONS - UPSERT DATA
	// ============================================================================

	// 5.1 Upsert existing item (will update)
	upsertEmbedding := make([]float32, dimension)
	for j := range upsertEmbedding {
		upsertEmbedding[j] = rand.Float32()
	}

	collection.Upsert(ctx, []string{ids[0]},
		[]string{"Upserted document (was updated)"},
		goseekdb.WithEmbeddings([][]float32{upsertEmbedding}),
		goseekdb.WithMetadatas([]goseekdb.Metadata{
			{"category": "AI", "upserted": true},
		}),
	)

	// 5.2 Upsert new item (will insert)
	newID := uuid.New().String()
	newEmbedding := make([]float32, dimension)
	for j := range newEmbedding {
		newEmbedding[j] = rand.Float32()
	}

	collection.Upsert(ctx, []string{newID},
		[]string{"This is a new document from upsert"},
		goseekdb.WithEmbeddings([][]float32{newEmbedding}),
		goseekdb.WithMetadatas([]goseekdb.Metadata{
			{"category": "New", "upserted": true},
		}),
	)

	// 5.3 Upsert multiple items (one existing, one new)
	upsertIDs := []string{ids[4], uuid.New().String()}
	upsertEmbeddings := make([][]float32, 2)
	for i := range upsertEmbeddings {
		upsertEmbeddings[i] = make([]float32, dimension)
		for j := range upsertEmbeddings[i] {
			upsertEmbeddings[i][j] = rand.Float32()
		}
	}

	collection.Upsert(ctx, upsertIDs,
		[]string{"Upserted doc 1", "Upserted doc 2"},
		goseekdb.WithEmbeddings(upsertEmbeddings),
		goseekdb.WithMetadatas([]goseekdb.Metadata{
			{"upserted": true},
			{"upserted": true},
		}),
	)

	// ============================================================================
	// PART 6: DQL OPERATIONS - QUERY (VECTOR SIMILARITY SEARCH)
	// ============================================================================

	// 6.1 Basic vector similarity query
	queryVector := embeddings[0] // Query with first document's vector
	results, err := collection.Query(ctx, nil, 3,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
	)
	if err != nil {
		log.Printf("Failed to query: %v", err)
	} else {
		fmt.Printf("Query results: %d items\n", len(results.IDs[0]))
	}

	// 6.2 Query with metadata filter (simplified equality)
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhere(goseekdb.Filter{"category": "AI"}),
	)

	// 6.3 Query with comparison operators
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhere(goseekdb.Filter{"score": goseekdb.Filter{"$gte": 90}}),
	)

	// 6.4 Query with $in operator
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhere(goseekdb.Filter{
			"tag": goseekdb.Filter{"$in": []interface{}{"ml", "python", "neural"}},
		}),
	)

	// 6.5 Query with logical operators ($or) - simplified equality
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhere(goseekdb.Filter{
			"$or": []interface{}{
				goseekdb.Filter{"category": "AI"},
				goseekdb.Filter{"tag": "python"},
			},
		}),
	)

	// 6.6 Query with logical operators ($and) - simplified equality
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhere(goseekdb.Filter{
			"$and": []interface{}{
				goseekdb.Filter{"category": "AI"},
				goseekdb.Filter{"score": goseekdb.Filter{"$gte": 90}},
			},
		}),
	)

	// 6.7 Query with document filter
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "machine learning"}),
	)

	// 6.8 Query with combined filters (simplified equality)
	results, _ = collection.Query(ctx, nil, 5,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithWhere(goseekdb.Filter{"category": "AI", "year": goseekdb.Filter{"$gte": 2023}}),
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "learning"}),
	)

	// 6.9 Query with multiple embeddings (batch query)
	batchEmbeddings := [][]float32{embeddings[0], embeddings[1]}
	batchResults, _ := collection.Query(ctx, nil, 2,
		goseekdb.WithQueryEmbeddings(batchEmbeddings),
	)
	// batchResults.IDs[0] contains results for first query
	// batchResults.IDs[1] contains results for second query
	_ = batchResults

	// 6.10 Query with specific fields
	results, _ = collection.Query(ctx, nil, 2,
		goseekdb.WithQueryEmbeddings([][]float32{queryVector}),
		goseekdb.WithInclude([]string{"documents", "metadatas", "embeddings"}),
	)
	_ = results

	// ============================================================================
	// PART 7: DQL OPERATIONS - GET (RETRIEVE BY IDS OR FILTERS)
	// ============================================================================

	// 7.1 Get by single ID
	result, _ := collection.Get(ctx, []string{ids[0]})
	// result.IDs contains [ids[0]]
	// result.Documents contains document for ids[0]
	_ = result

	// 7.2 Get by multiple IDs
	results2, _ := collection.Get(ctx, ids[:3])
	// results2.IDs contains ids[:3]
	// results2.Documents contains documents for all IDs
	_ = results2

	// 7.3 Get by metadata filter (simplified equality)
	results2, _ = collection.Get(ctx, nil,
		goseekdb.WithGetWhere(goseekdb.Filter{"category": "AI"}),
		goseekdb.WithLimit(5),
	)

	// 7.4 Get with comparison operators
	results2, _ = collection.Get(ctx, nil,
		goseekdb.WithGetWhere(goseekdb.Filter{"score": goseekdb.Filter{"$gte": 90}}),
		goseekdb.WithLimit(5),
	)

	// 7.5 Get with $in operator
	results2, _ = collection.Get(ctx, nil,
		goseekdb.WithGetWhere(goseekdb.Filter{
			"tag": goseekdb.Filter{"$in": []interface{}{"ml", "python"}},
		}),
		goseekdb.WithLimit(5),
	)

	// 7.6 Get with logical operators (simplified equality)
	results2, _ = collection.Get(ctx, nil,
		goseekdb.WithGetWhere(goseekdb.Filter{
			"$or": []interface{}{
				goseekdb.Filter{"category": "AI"},
				goseekdb.Filter{"category": "Programming"},
			},
		}),
		goseekdb.WithLimit(5),
	)

	// 7.7 Get by document filter
	results2, _ = collection.Get(ctx, nil,
		goseekdb.WithGetWhereDocument(goseekdb.Filter{"$contains": "Python"}),
		goseekdb.WithLimit(5),
	)

	// 7.8 Get with pagination
	resultsPage1, _ := collection.Get(ctx, nil,
		goseekdb.WithLimit(2),
		goseekdb.WithOffset(0),
	)
	resultsPage2, _ := collection.Get(ctx, nil,
		goseekdb.WithLimit(2),
		goseekdb.WithOffset(2),
	)
	_, _ = resultsPage1, resultsPage2

	// 7.9 Get with specific fields
	results2, _ = collection.Get(ctx, ids[:2],
		goseekdb.WithGetInclude([]string{"documents", "metadatas", "embeddings"}),
	)

	// 7.10 Get all data
	allResults, _ := collection.Get(ctx, nil,
		goseekdb.WithLimit(100),
	)
	_ = allResults

	// ============================================================================
	// PART 8: DQL OPERATIONS - HYBRID SEARCH
	// ============================================================================

	// 8.1 Hybrid search with full-text and vector search
	// Note: This requires query_embeddings to be provided directly
	// In real usage, you might have an embedding function
	hybridResults, err := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "machine learning"},
			Where:         goseekdb.Filter{"category": "AI"}, // Simplified equality
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryEmbeddings: [][]float32{embeddings[0]},
			Where:           goseekdb.Filter{"year": goseekdb.Filter{"$gte": 2022}},
			NResults:        10,
		},
		&goseekdb.HybridSearchRank{
			RRF: &goseekdb.RRFConfig{}, // Reciprocal Rank Fusion
		},
		5,
	)
	// hybridResults.IDs contains IDs for the hybrid search
	// hybridResults.Documents contains documents for the hybrid search
	if err != nil {
		log.Printf("Failed hybrid search: %v", err)
	} else {
		fmt.Printf("Hybrid search: %d results\n", len(hybridResults.IDs))
	}

	// ============================================================================
	// PART 9: DML OPERATIONS - DELETE DATA
	// ============================================================================

	// 9.1 Delete by IDs
	deleteIDs := []string{vectorOnlyIDs[0], newID}
	collection.Delete(ctx, deleteIDs, nil, nil)

	// 9.2 Delete by metadata filter
	collection.Delete(ctx, nil,
		goseekdb.Filter{"type": goseekdb.Filter{"$eq": "vector_only"}},
		nil,
	)

	// 9.3 Delete by document filter
	collection.Delete(ctx, nil, nil,
		goseekdb.Filter{"$contains": "Updated document"},
	)

	// 9.4 Delete with combined filters
	collection.Delete(ctx, nil,
		goseekdb.Filter{"category": goseekdb.Filter{"$eq": "CV"}},
		goseekdb.Filter{"$contains": "vision"},
	)

	// ============================================================================
	// PART 10: COLLECTION INFORMATION
	// ============================================================================

	// 10.1 Get collection count
	count, _ := collection.Count(ctx)
	fmt.Printf("Collection count: %d items\n", count)

	// 10.2 Preview first few items in collection (returns all columns by default)
	preview, _ := collection.Peek(ctx, 5)
	fmt.Printf("Preview: %d items\n", len(preview.IDs))
	for i := range preview.IDs {
		doc := ""
		if i < len(preview.Documents) {
			doc = preview.Documents[i]
		}
		fmt.Printf("  ID: %s, Document: %s\n", preview.IDs[i], doc)

		meta := goseekdb.Metadata{}
		if i < len(preview.Metadatas) {
			meta = preview.Metadatas[i]
		}
		embDim := 0
		if i < len(preview.Embeddings) && preview.Embeddings[i] != nil {
			embDim = len(preview.Embeddings[i])
		}
		fmt.Printf("  Metadata: %+v, Embedding dim: %d\n", meta, embDim)
	}

	// 10.3 Count collections in database
	collectionCount, _ := client.CountCollections(ctx)
	fmt.Printf("Database has %d collections\n", collectionCount)

	// ============================================================================
	// PART 11: CLEANUP
	// ============================================================================

	// Delete test collections
	if err := client.DeleteCollection(ctx, "another_collection"); err != nil {
		fmt.Printf("Could not delete 'another_collection': %v\n", err)
	}

	// Uncomment to delete main collection
	client.DeleteCollection(ctx, collectionName)
}
