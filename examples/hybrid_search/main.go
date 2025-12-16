package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/ob-labs/seekdb-go"
)

// This example demonstrates the power of hybrid search by comparing
// query() (vector-only) vs HybridSearch() (full-text + vector with RRF fusion)

func main() {
	ctx := context.Background()

	client, err := goseekdb.NewClient(
		goseekdb.WithHost("localhost"),
		goseekdb.WithPort(2881),
		goseekdb.WithDatabase("test"),
		goseekdb.WithUser("root"),
		goseekdb.WithPassword(""),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Delete collection if it exists (for clean test runs)
	_ = client.DeleteCollection(ctx, "hybrid_search_demo")

	// Create collection
	collection, err := client.CreateCollection(ctx, "hybrid_search_demo")
	if err != nil {
		log.Fatalf("Failed to create collection: %v", err)
	}

	// Sample data
	documents := []string{
		"Machine learning is revolutionizing artificial intelligence and data science",
		"Python programming language is essential for machine learning developers",
		"Deep learning neural networks enable advanced AI applications",
		"Data science combines statistics, programming, and domain expertise",
		"Natural language processing uses machine learning to understand text",
		"Computer vision algorithms process images using deep learning techniques",
		"Reinforcement learning trains agents through reward-based feedback",
		"Python libraries like TensorFlow and PyTorch simplify machine learning",
		"Artificial intelligence systems can learn from large datasets",
		"Neural networks mimic the structure of biological brain connections",
	}

	ids := make([]string, len(documents))
	metadatas := []goseekdb.Metadata{
		{"category": "AI", "topic": "machine learning", "year": 2023, "popularity": 95},
		{"category": "Programming", "topic": "python", "year": 2023, "popularity": 88},
		{"category": "AI", "topic": "deep learning", "year": 2024, "popularity": 92},
		{"category": "Data Science", "topic": "data analysis", "year": 2023, "popularity": 85},
		{"category": "AI", "topic": "nlp", "year": 2024, "popularity": 90},
		{"category": "AI", "topic": "computer vision", "year": 2023, "popularity": 87},
		{"category": "AI", "topic": "reinforcement learning", "year": 2024, "popularity": 89},
		{"category": "Programming", "topic": "python", "year": 2023, "popularity": 91},
		{"category": "AI", "topic": "general ai", "year": 2023, "popularity": 93},
		{"category": "AI", "topic": "neural networks", "year": 2024, "popularity": 94},
	}

	for i := range documents {
		ids[i] = fmt.Sprintf("doc_%d", i+1)
	}

	err = collection.Add(ctx, ids, documents, goseekdb.WithMetadatas(metadatas))
	if err != nil {
		log.Fatalf("Failed to add documents: %v", err)
	}

	separator := strings.Repeat("=", 100)

	// SCENARIO 1
	fmt.Println(separator)
	fmt.Println("SCENARIO 1: Keyword + Semantic Search")
	fmt.Println(separator)
	fmt.Println("Goal: Find documents similar to 'AI research' AND containing 'machine learning'")
	fmt.Println()

	// query() approach - vector search with document filter
	queryResult1, err := collection.Query(ctx,
		[]string{"AI research"},
		5,
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "machine learning"}),
	)
	if err != nil {
		log.Printf("Query failed: %v", err)
	}

	// hybrid_search() approach - combines both searches
	hybridResult1, err := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "machine learning"},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"AI research"},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{
			RRF: &goseekdb.RRFConfig{},
		},
		5,
	)
	if err != nil {
		log.Printf("Hybrid search failed: %v", err)
	}

	fmt.Println("query() Results:")
	if queryResult1 != nil && len(queryResult1.IDs) > 0 {
		for i, id := range queryResult1.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results:")
	if hybridResult1 != nil {
		for i, id := range hybridResult1.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  hybrid_search() correctly prioritizes documents that explicitly contain")
	fmt.Println("  'machine learning' (from full-text search) while also being semantically")
	fmt.Println("  relevant to 'AI research' (from vector search). The RRF fusion ensures")
	fmt.Println("  documents matching both criteria rank higher.")

	//SCENARIO 2
	fmt.Println("\n" + separator)
	fmt.Println("SCENARIO 2: Independent Filters for Different Search Types")
	fmt.Println(separator)
	fmt.Println("Goal: Full-text='neural' (year=2024) + Vector='deep learning' (popularity>=90)")
	fmt.Println()

	// query() - same filter applies to both conditions
	queryResult2, _ := collection.Query(ctx,
		[]string{"deep learning"},
		5,
		goseekdb.WithWhere(goseekdb.Filter{
			"year":       goseekdb.Filter{"$eq": 2024},
			"popularity": goseekdb.Filter{"$gte": 90},
		}),
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "neural"}),
	)

	// hybrid_search() - different filters for each search type
	hybridResult2, _ := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "neural"},
			Where:         goseekdb.Filter{"year": goseekdb.Filter{"$eq": 2024}},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"deep learning"},
			Where:      goseekdb.Filter{"popularity": goseekdb.Filter{"$gte": 90}},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{RRF: &goseekdb.RRFConfig{}},
		5,
	)

	fmt.Println("query() Results (same filter for both):")
	if queryResult2 != nil && len(queryResult2.IDs) > 0 {
		for i, id := range queryResult2.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
			fmt.Printf("      %+v\n", metadatas[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results (independent filters):")
	if hybridResult2 != nil {
		for i, id := range hybridResult2.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
			fmt.Printf("      %+v\n", metadatas[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  query() only returns limited results because it requires documents to satisfy BOTH")
	fmt.Println("  year=2024 AND popularity>=90 simultaneously. hybrid_search() returns more results")
	fmt.Println("  by applying year=2024 filter to full-text search and popularity>=90 filter to")
	fmt.Println("  vector search independently, then fusing the results.")

	//SCENARIO 3
	fmt.Println("\n" + separator)
	fmt.Println("SCENARIO 3: Combining Multiple Search Strategies")
	fmt.Println(separator)
	fmt.Println("Goal: Find documents about 'machine learning algorithms'")
	fmt.Println()

	// query() - vector search only
	queryResult3, _ := collection.Query(ctx,
		[]string{"machine learning algorithms"},
		5,
	)

	// hybrid_search() - combines full-text and vector
	hybridResult3, _ := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "machine learning"},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"machine learning algorithms"},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{RRF: &goseekdb.RRFConfig{}},
		5,
	)

	fmt.Println("query() Results (vector similarity only):")
	if queryResult3 != nil && len(queryResult3.IDs) > 0 {
		for i, id := range queryResult3.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results (full-text + vector fusion):")
	if hybridResult3 != nil {
		for i, id := range hybridResult3.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  hybrid_search() combines full-text search (for 'machine learning') with vector")
	fmt.Println("  search (for semantic similarity to 'machine learning algorithms'), ensuring that")
	fmt.Println("  documents containing the exact keyword rank higher while still capturing")
	fmt.Println("  semantically relevant content.")

	// SCENARIO 4
	fmt.Println("\n" + separator)
	fmt.Println("SCENARIO 4: Complex Multi-Criteria Search")
	fmt.Println(separator)
	fmt.Println("Goal: Full-text='learning' (category=AI) + Vector='artificial intelligence' (year>=2023)")
	fmt.Println()

	// query() - limited to single search with combined filters
	queryResult4, _ := collection.Query(ctx,
		[]string{"artificial intelligence"},
		5,
		goseekdb.WithWhere(goseekdb.Filter{
			"category": goseekdb.Filter{"$eq": "AI"},
			"year":     goseekdb.Filter{"$gte": 2023},
		}),
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "learning"}),
	)

	// hybrid_search() - separate criteria for each search type
	hybridResult4, _ := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "learning"},
			Where:         goseekdb.Filter{"category": goseekdb.Filter{"$eq": "AI"}},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"artificial intelligence"},
			Where:      goseekdb.Filter{"year": goseekdb.Filter{"$gte": 2023}},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{RRF: &goseekdb.RRFConfig{}},
		5,
	)

	fmt.Println("query() Results:")
	if queryResult4 != nil && len(queryResult4.IDs) > 0 {
		for i, id := range queryResult4.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
			fmt.Printf("      %+v\n", metadatas[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results:")
	if hybridResult4 != nil {
		for i, id := range hybridResult4.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
			fmt.Printf("      %+v\n", metadatas[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  hybrid_search() provides better ranking by prioritizing documents that score")
	fmt.Println("  highly in both full-text search (containing 'learning' with category=AI) and")
	fmt.Println("  vector search (semantically similar to 'artificial intelligence' with year>=2023).")

	// SCENARIO 5
	fmt.Println("\n" + separator)
	fmt.Println("SCENARIO 5: Result Quality - RRF Fusion")
	fmt.Println(separator)
	fmt.Println("Goal: Search for 'Python machine learning'")
	fmt.Println()

	// query() - single ranking
	queryResult5, _ := collection.Query(ctx,
		[]string{"Python machine learning"},
		5,
	)

	// hybrid_search() - RRF fusion of multiple rankings
	hybridResult5, _ := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "Python"},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"Python machine learning"},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{RRF: &goseekdb.RRFConfig{}},
		5,
	)

	fmt.Println("query() Results (single ranking):")
	if queryResult5 != nil && len(queryResult5.IDs) > 0 {
		for i, id := range queryResult5.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results (RRF fusion):")
	if hybridResult5 != nil {
		for i, id := range hybridResult5.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  RRF (Reciprocal Rank Fusion) combines rankings from full-text search (for 'Python')")
	fmt.Println("  and vector search (for 'Python machine learning'). RRF provides more stable and")
	fmt.Println("  robust ranking by considering multiple signals.")

	// SCENARIO 6
	fmt.Println("\n" + separator)
	fmt.Println("SCENARIO 6: Different Filter Criteria for Each Search")
	fmt.Println(separator)
	fmt.Println("Goal: Full-text='neural' (high popularity) + Vector='deep learning' (recent year)")
	fmt.Println()

	// query() - cannot separate filters for keyword vs semantic
	queryResult6, _ := collection.Query(ctx,
		[]string{"deep learning"},
		5,
		goseekdb.WithWhere(goseekdb.Filter{
			"popularity": goseekdb.Filter{"$gte": 90},
			"year":       goseekdb.Filter{"$gte": 2023},
		}),
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "neural"}),
	)

	// hybrid_search() - different filters for keyword search vs semantic search
	hybridResult6, _ := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "neural"},
			Where:         goseekdb.Filter{"popularity": goseekdb.Filter{"$gte": 90}},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"deep learning"},
			Where:      goseekdb.Filter{"year": goseekdb.Filter{"$gte": 2023}},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{RRF: &goseekdb.RRFConfig{}},
		5,
	)

	fmt.Println("query() Results:")
	if queryResult6 != nil && len(queryResult6.IDs) > 0 {
		for i, id := range queryResult6.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
			fmt.Printf("      %+v\n", metadatas[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results:")
	if hybridResult6 != nil {
		for i, id := range hybridResult6.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
			fmt.Printf("      %+v\n", metadatas[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  query() returns limited results because it requires documents to satisfy BOTH")
	fmt.Println("  popularity>=90 AND year>=2023 simultaneously. hybrid_search() returns more results")
	fmt.Println("  by applying popularity>=90 filter to full-text search and year>=2023 filter to")
	fmt.Println("  vector search independently.")

	// SCENARIO 7
	fmt.Println("\n" + separator)
	fmt.Println("SCENARIO 7: Partial Keyword Match + Semantic Similarity")
	fmt.Println(separator)
	fmt.Println("Goal: Documents containing 'Python' + Semantically similar to 'data science'")
	fmt.Println()

	// query() - filter applied after vector search
	queryResult7, _ := collection.Query(ctx,
		[]string{"data science"},
		5,
		goseekdb.WithWhereDocument(goseekdb.Filter{"$contains": "Python"}),
	)

	// hybrid_search() - parallel searches then fusion
	hybridResult7, _ := collection.HybridSearch(ctx,
		&goseekdb.HybridSearchQuery{
			WhereDocument: goseekdb.Filter{"$contains": "Python"},
			NResults:      10,
		},
		&goseekdb.HybridSearchKNN{
			QueryTexts: []string{"data science"},
			NResults:   10,
		},
		&goseekdb.HybridSearchRank{RRF: &goseekdb.RRFConfig{}},
		5,
	)

	fmt.Println("query() Results:")
	if queryResult7 != nil && len(queryResult7.IDs) > 0 {
		for i, id := range queryResult7.IDs[0] {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nhybrid_search() Results:")
	if hybridResult7 != nil {
		for i, id := range hybridResult7.IDs {
			idx := findIndex(ids, id)
			fmt.Printf("  %d. %s\n", i+1, documents[idx])
		}
	}

	fmt.Println("\nAnalysis:")
	fmt.Println("  query() returns limited results because it first performs vector search for")
	fmt.Println("  'data science', then filters to documents containing 'Python'. hybrid_search()")
	fmt.Println("  runs full-text search (for 'Python') and vector search (for 'data science') in")
	fmt.Println("  parallel, then fuses the results for better recall and more comprehensive results.")

	// Cleanup
	fmt.Println("\nCleaning up...")
	err = client.DeleteCollection(ctx, "hybrid_search_demo")
	if err != nil {
		log.Printf("Failed to cleanup: %v", err)
	}

	// SUMMARY
	fmt.Println("\n" + separator)
	fmt.Println("SUMMARY")
	fmt.Println(separator)
	fmt.Println()
	fmt.Println(`query() limitations:
  - Single search type (vector similarity)
  - Filters applied after search (may miss relevant docs)
  - Cannot combine full-text and vector search results
  - Same filter criteria for all conditions

hybrid_search() advantages:
  - Simultaneous full-text + vector search
  - Independent filters for each search type
  - Intelligent result fusion using RRF
  - Better recall for complex queries
  - Handles scenarios requiring both keyword and semantic matching`)

	fmt.Println("Hybrid search example completed!")
}

func findIndex(ids []string, target string) int {
	for i, id := range ids {
		if id == target {
			return i
		}
	}
	return 0
}
