package goseekdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ob-labs/seekdb-go/embedding"
)

// collectionQuery implements the Query operation for collections.
func (c *Client) collectionQuery(ctx context.Context, collectionName string, queryTexts []string, nResults int, opts *QueryOptions, embFunc embedding.EmbeddingFunc, distance DistanceMetric) (*QueryResult, error) {
	// If query embeddings are provided, use them directly. If not, generate them from query texts.
	var queryEmbeddings [][]float32
	if opts.QueryEmbeddings != nil {
		queryEmbeddings = opts.QueryEmbeddings
	} else if len(queryTexts) > 0 {
		if embFunc == nil {
			return nil, ErrEmbeddingFunctionRequired
		}
		var err error
		queryEmbeddings, err = embFunc.Embed(queryTexts)
		if err != nil {
			return nil, fmt.Errorf("failed to generate query embeddings: %w", err)
		}
	} else {
		return nil, fmt.Errorf("%w: must provide query_texts or query_embeddings", ErrInvalidParameter)
	}

	tableName := GetTableName(collectionName)
	result := &QueryResult{
		IDs:        make([][]string, len(queryEmbeddings)),
		Distances:  make([][]float64, len(queryEmbeddings)),
		Documents:  make([][]string, len(queryEmbeddings)),
		Metadatas:  make([][]Metadata, len(queryEmbeddings)),
		Embeddings: make([][][]float32, len(queryEmbeddings)),
	}

	// Execute query for each embedding
	for i, queryEmb := range queryEmbeddings {
		// Build WHERE clause from filters
		var conditions []string
		var args []interface{}

		if opts.Where != nil {
			clause, filterArgs, err := c.filterBuilder.BuildMetadataFilter(opts.Where)
			if err != nil {
				return nil, err
			}
			if clause != "" {
				conditions = append(conditions, clause)
				args = append(args, filterArgs...)
			}
		}

		if opts.WhereDocument != nil {
			clause, filterArgs, err := c.filterBuilder.BuildDocumentFilter(opts.WhereDocument)
			if err != nil {
				return nil, err
			}
			if clause != "" {
				conditions = append(conditions, clause)
				args = append(args, filterArgs...)
			}
		}

		whereClause := ""
		if len(conditions) > 0 {
			whereClause = "WHERE " + strings.Join(conditions, " AND ")
		}

		// Build vector search query
		// Note: Actual syntax depends on SeekDB's vector search implementation
		// Use the appropriate distance function based on the collection's distance metric
		distanceFunc := distance.DistanceFuncName()

		// Convert vector to string format for SQL (embed directly in query like Python version)
		vectorStr := vectorToString(queryEmb)

		// Build SQL query with vector distance calculation embedded directly as string literal
		querySQL := fmt.Sprintf(`
			SELECT %s, %s, %s, %s,
			       %s(%s, '%s') AS distance
			FROM %s
			%s
			ORDER BY %s(%s, '%s')
			APPROXIMATE
			LIMIT ?
		`, FieldID, FieldDocument, FieldMetadata, FieldEmbedding,
			distanceFunc, FieldEmbedding, vectorStr, tableName, whereClause, distanceFunc, FieldEmbedding, vectorStr)

		queryArgs := append(args, nResults)
		rows, err := c.conn.Query(ctx, querySQL, queryArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to query collection: %w", err)
		}

		ids, distances, documents, metadatas, embeddings, err := c.scanQueryResults(rows)
		rows.Close()
		if err != nil {
			return nil, err
		}

		result.IDs[i] = ids
		result.Distances[i] = distances
		result.Documents[i] = documents
		result.Metadatas[i] = metadatas
		result.Embeddings[i] = embeddings
	}

	return result, nil
}

// collectionGet implements the Get operation for collections.
func (c *Client) collectionGet(ctx context.Context, collectionName string, ids []string, opts *GetOptions) (*GetResult, error) {
	tableName := GetTableName(collectionName)

	var conditions []string
	var args []interface{}

	// Filter by IDs
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		conditions = append(conditions, fmt.Sprintf("%s IN (%s)", FieldID, strings.Join(placeholders, ", ")))
	}

	// Add metadata filter
	if opts.Where != nil {
		clause, filterArgs, err := c.filterBuilder.BuildMetadataFilter(opts.Where)
		if err != nil {
			return nil, err
		}
		if clause != "" {
			conditions = append(conditions, clause)
			args = append(args, filterArgs...)
		}
	}

	// Add document filter
	if opts.WhereDocument != nil {
		clause, filterArgs, err := c.filterBuilder.BuildDocumentFilter(opts.WhereDocument)
		if err != nil {
			return nil, err
		}
		if clause != "" {
			conditions = append(conditions, clause)
			args = append(args, filterArgs...)
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	querySQL := fmt.Sprintf(`
		SELECT %s, %s, %s, %s
		FROM %s
		%s
		LIMIT ? OFFSET ?
	`, FieldID, FieldDocument, FieldMetadata, FieldEmbedding, tableName, whereClause)

	limit := opts.Limit
	if limit == 0 {
		limit = 1000 // Default limit
	}

	queryArgs := append(args, limit, opts.Offset)
	rows, err := c.conn.Query(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to get documents: %w", err)
	}
	defer rows.Close()

	var result GetResult
	for rows.Next() {
		var id, document, metadataJSON, embeddingJSON string
		if err := rows.Scan(&id, &document, &metadataJSON, &embeddingJSON); err != nil {
			return nil, err
		}

		result.IDs = append(result.IDs, id)
		result.Documents = append(result.Documents, document)

		var metadata Metadata
		if err := metadata.FromJSON(metadataJSON); err == nil {
			result.Metadatas = append(result.Metadatas, metadata)
		}

		var embedding []float32
		if err := json.Unmarshal([]byte(embeddingJSON), &embedding); err == nil {
			result.Embeddings = append(result.Embeddings, embedding)
		}
	}

	return &result, nil
}

// collectionCount implements the Count operation for collections.
func (c *Client) collectionCount(ctx context.Context, collectionName string) (int, error) {
	tableName := GetTableName(collectionName)
	querySQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)

	row := c.conn.QueryRow(ctx, querySQL)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	return count, nil
}

// collectionHybridSearch implements hybrid search combining full-text and vector search
// using DBMS_HYBRID_SEARCH.GET_SQL to generate and execute the query.
func (c *Client) collectionHybridSearch(ctx context.Context, collectionName string, query *HybridSearchQuery, knn *HybridSearchKNN, rank *HybridSearchRank, nResults int, embFunc embedding.EmbeddingFunc, distance DistanceMetric) (*HybridSearchResult, error) {
	tableName := GetTableName(collectionName)

	// Build search_parm JSON
	searchParm, err := c.buildSearchParm(query, knn, rank, nResults, embFunc)
	if err != nil {
		return nil, fmt.Errorf("failed to build search_parm: %w", err)
	}

	// Convert search_parm to JSON string
	searchParmBytes, err := json.Marshal(searchParm)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search_parm: %w", err)
	}
	searchParmJSON := string(searchParmBytes)

	// Escape single quotes for SQL
	escapedParams := strings.ReplaceAll(searchParmJSON, "'", "''")

	// Use a transaction to ensure SET and SELECT use the same connection
	// This is necessary because @search_parm is a session variable
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Set the search_parm variable
	setSQL := fmt.Sprintf("SET @search_parm = '%s'", escapedParams)
	_, err = tx.Execute(ctx, setSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to set search_parm: %w", err)
	}

	// Get SQL query from DBMS_HYBRID_SEARCH.GET_SQL
	getSQLQuery := fmt.Sprintf("SELECT DBMS_HYBRID_SEARCH.GET_SQL('%s', @search_parm) as query_sql FROM dual", tableName)
	row := tx.QueryRow(ctx, getSQLQuery)

	var querySQL sql.NullString
	if err := row.Scan(&querySQL); err != nil {
		return nil, fmt.Errorf("failed to get SQL from DBMS_HYBRID_SEARCH.GET_SQL: %w", err)
	}

	if !querySQL.Valid || querySQL.String == "" {
		// No SQL query returned, return empty results
		return &HybridSearchResult{
			IDs:        []string{},
			Distances:  []float64{},
			Documents:  []string{},
			Metadatas:  []Metadata{},
			Embeddings: [][]float32{},
		}, nil
	}

	// Remove any surrounding quotes if present
	finalSQL := strings.Trim(strings.TrimSpace(querySQL.String), "'\"")

	// Execute the returned SQL query
	rows, err := tx.Query(ctx, finalSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute hybrid search query: %w", err)
	}
	defer rows.Close()

	// Transform results
	result, err := c.transformHybridSearchResults(rows)
	if err != nil {
		return nil, err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// buildSearchParm builds the search_parm JSON from query, knn, and rank parameters.
func (c *Client) buildSearchParm(query *HybridSearchQuery, knn *HybridSearchKNN, rank *HybridSearchRank, nResults int, embFunc embedding.EmbeddingFunc) (map[string]interface{}, error) {
	searchParm := make(map[string]interface{})

	// Build query part (full-text search or scalar query)
	if query != nil {
		queryExpr := c.buildQueryExpression(query)
		if queryExpr != nil {
			searchParm["query"] = queryExpr
		}
	}

	// Build knn part (vector search)
	if knn != nil {
		knnExpr, err := c.buildKNNExpression(knn, embFunc)
		if err != nil {
			return nil, err
		}
		if knnExpr != nil {
			searchParm["knn"] = knnExpr
		}
	}

	// Set size
	if nResults > 0 {
		searchParm["size"] = nResults
	}

	// Build rank part
	if rank != nil {
		rankExpr := make(map[string]interface{})
		if rank.RRF != nil {
			rrfExpr := make(map[string]interface{})
			if rank.RRF.K > 0 {
				rrfExpr["rank_constant"] = rank.RRF.K
			}
			rankExpr["rrf"] = rrfExpr
		}
		if len(rankExpr) > 0 {
			searchParm["rank"] = rankExpr
		}
	}

	return searchParm, nil
}

// buildQueryExpression builds the query expression from HybridSearchQuery.
func (c *Client) buildQueryExpression(query *HybridSearchQuery) map[string]interface{} {
	whereDocument := query.WhereDocument
	where := query.Where

	// Case 1: Scalar query (metadata filtering only, no full-text search)
	if len(whereDocument) == 0 && len(where) > 0 {
		filterConditions := c.buildMetadataFilterForSearchParm(where)
		if len(filterConditions) > 0 {
			if len(filterConditions) == 1 {
				filterCond := filterConditions[0]
				// Check if it's a range query
				if _, ok := filterCond["range"]; ok {
					return filterCond
				}
				// Check if it's a term query
				if _, ok := filterCond["term"]; ok {
					return filterCond
				}
				// Otherwise, wrap in bool filter
				return map[string]interface{}{
					"bool": map[string]interface{}{
						"filter": filterConditions,
					},
				}
			}
			// Multiple filter conditions, wrap in bool
			return map[string]interface{}{
				"bool": map[string]interface{}{
					"filter": filterConditions,
				},
			}
		}
	}

	// Case 2: Full-text search (with or without metadata filtering)
	if len(whereDocument) > 0 {
		docQuery := c.buildDocumentQuery(whereDocument)
		if docQuery != nil {
			filterConditions := c.buildMetadataFilterForSearchParm(where)
			if len(filterConditions) > 0 {
				// Full-text search with metadata filtering
				return map[string]interface{}{
					"bool": map[string]interface{}{
						"must":   []interface{}{docQuery},
						"filter": filterConditions,
					},
				}
			}
			// Full-text search only
			return docQuery
		}
	}

	return nil
}

// buildDocumentQuery builds document query from where_document condition using query_string.
func (c *Client) buildDocumentQuery(whereDocument Filter) map[string]interface{} {
	if len(whereDocument) == 0 {
		return nil
	}

	// Handle $contains - use query_string
	if contains, ok := whereDocument["$contains"]; ok {
		return map[string]interface{}{
			"query_string": map[string]interface{}{
				"fields": []string{"document"},
				"query":  contains,
			},
		}
	}

	// Handle $and with $contains
	if andConditions, ok := whereDocument["$and"]; ok {
		if conditions, ok := andConditions.([]interface{}); ok {
			var containsQueries []string
			for _, cond := range conditions {
				if condMap, ok := cond.(map[string]interface{}); ok {
					if contains, ok := condMap["$contains"]; ok {
						if containsStr, ok := contains.(string); ok {
							containsQueries = append(containsQueries, containsStr)
						}
					}
				}
			}
			if len(containsQueries) > 0 {
				return map[string]interface{}{
					"query_string": map[string]interface{}{
						"fields": []string{"document"},
						"query":  strings.Join(containsQueries, " "),
					},
				}
			}
		}
	}

	// Handle $or with $contains
	if orConditions, ok := whereDocument["$or"]; ok {
		if conditions, ok := orConditions.([]interface{}); ok {
			var containsQueries []string
			for _, cond := range conditions {
				if condMap, ok := cond.(map[string]interface{}); ok {
					if contains, ok := condMap["$contains"]; ok {
						if containsStr, ok := contains.(string); ok {
							containsQueries = append(containsQueries, containsStr)
						}
					}
				}
			}
			if len(containsQueries) > 0 {
				return map[string]interface{}{
					"query_string": map[string]interface{}{
						"fields": []string{"document"},
						"query":  strings.Join(containsQueries, " OR "),
					},
				}
			}
		}
	}

	return nil
}

// buildMetadataFilterForSearchParm builds metadata filter conditions for search_parm.
func (c *Client) buildMetadataFilterForSearchParm(where Filter) []map[string]interface{} {
	if len(where) == 0 {
		return nil
	}
	return c.buildMetadataFilterConditions(where)
}

// buildMetadataFilterConditions recursively builds metadata filter conditions from nested dictionary.
func (c *Client) buildMetadataFilterConditions(condition Filter) []map[string]interface{} {
	if len(condition) == 0 {
		return nil
	}

	var result []map[string]interface{}

	// Handle logical operators
	if andConditions, ok := condition["$and"]; ok {
		if conditions, ok := andConditions.([]interface{}); ok {
			var mustConditions []map[string]interface{}
			for _, subCond := range conditions {
				if subCondMap, ok := subCond.(map[string]interface{}); ok {
					subFilters := c.buildMetadataFilterConditions(Filter(subCondMap))
					mustConditions = append(mustConditions, subFilters...)
				}
			}
			if len(mustConditions) > 0 {
				result = append(result, map[string]interface{}{
					"bool": map[string]interface{}{
						"must": mustConditions,
					},
				})
			}
		}
		return result
	}

	if orConditions, ok := condition["$or"]; ok {
		if conditions, ok := orConditions.([]interface{}); ok {
			var shouldConditions []map[string]interface{}
			for _, subCond := range conditions {
				if subCondMap, ok := subCond.(map[string]interface{}); ok {
					subFilters := c.buildMetadataFilterConditions(Filter(subCondMap))
					shouldConditions = append(shouldConditions, subFilters...)
				}
			}
			if len(shouldConditions) > 0 {
				result = append(result, map[string]interface{}{
					"bool": map[string]interface{}{
						"should": shouldConditions,
					},
				})
			}
		}
		return result
	}

	if notCondition, ok := condition["$not"]; ok {
		if notCondMap, ok := notCondition.(map[string]interface{}); ok {
			notFilters := c.buildMetadataFilterConditions(Filter(notCondMap))
			if len(notFilters) > 0 {
				result = append(result, map[string]interface{}{
					"bool": map[string]interface{}{
						"must_not": notFilters,
					},
				})
			}
		}
		return result
	}

	// Handle field conditions
	for key, value := range condition {
		if key == "$and" || key == "$or" || key == "$not" {
			continue
		}

		// Build field name with JSON_EXTRACT format
		fieldName := fmt.Sprintf("(JSON_EXTRACT(metadata, '$.%s'))", key)

		// Handle both map[string]interface{} and Filter types
		var valueMap map[string]interface{}
		var isValueMap bool
		if m, ok := value.(map[string]interface{}); ok {
			valueMap, isValueMap = m, true
		} else if f, ok := value.(Filter); ok {
			valueMap, isValueMap = f, true
		}

		if isValueMap {
			// Handle comparison operators
			rangeConditions := make(map[string]interface{})
			var termValue interface{}

			for op, opValue := range valueMap {
				switch op {
				case "$eq":
					termValue = opValue
				case "$ne":
					result = append(result, map[string]interface{}{
						"bool": map[string]interface{}{
							"must_not": []map[string]interface{}{
								{"term": map[string]interface{}{fieldName: opValue}},
							},
						},
					})
				case "$lt":
					rangeConditions["lt"] = opValue
				case "$lte":
					rangeConditions["lte"] = opValue
				case "$gt":
					rangeConditions["gt"] = opValue
				case "$gte":
					rangeConditions["gte"] = opValue
				case "$in":
					if inValues, ok := opValue.([]interface{}); ok {
						var inConditions []map[string]interface{}
						for _, val := range inValues {
							inConditions = append(inConditions, map[string]interface{}{
								"term": map[string]interface{}{fieldName: val},
							})
						}
						if len(inConditions) > 0 {
							result = append(result, map[string]interface{}{
								"bool": map[string]interface{}{
									"should": inConditions,
								},
							})
						}
					}
				case "$nin":
					if ninValues, ok := opValue.([]interface{}); ok {
						var ninConditions []map[string]interface{}
						for _, val := range ninValues {
							ninConditions = append(ninConditions, map[string]interface{}{
								"term": map[string]interface{}{fieldName: val},
							})
						}
						if len(ninConditions) > 0 {
							result = append(result, map[string]interface{}{
								"bool": map[string]interface{}{
									"must_not": ninConditions,
								},
							})
						}
					}
				}
			}

			if len(rangeConditions) > 0 {
				result = append(result, map[string]interface{}{
					"range": map[string]interface{}{fieldName: rangeConditions},
				})
			} else if termValue != nil {
				result = append(result, map[string]interface{}{
					"term": map[string]interface{}{fieldName: termValue},
				})
			}
		} else {
			// Direct equality
			result = append(result, map[string]interface{}{
				"term": map[string]interface{}{fieldName: value},
			})
		}
	}

	return result
}

// buildKNNExpression builds the knn expression from HybridSearchKNN.
func (c *Client) buildKNNExpression(knn *HybridSearchKNN, embFunc embedding.EmbeddingFunc) (map[string]interface{}, error) {
	var queryVector []float32

	// Handle vector generation
	if knn.QueryEmbeddings != nil && len(knn.QueryEmbeddings) > 0 {
		// Use first query embedding
		queryVector = knn.QueryEmbeddings[0]
	} else if len(knn.QueryTexts) > 0 {
		if embFunc == nil {
			return nil, fmt.Errorf("knn.query_texts provided but no embedding function: %w", ErrEmbeddingFunctionRequired)
		}
		embeddings, err := embFunc.Embed(knn.QueryTexts)
		if err != nil {
			return nil, fmt.Errorf("failed to generate embeddings from query_texts: %w", err)
		}
		if len(embeddings) > 0 {
			queryVector = embeddings[0]
		}
	} else {
		return nil, fmt.Errorf("knn requires either query_embeddings or query_texts")
	}

	if len(queryVector) == 0 {
		return nil, nil
	}

	// Convert []float32 to []interface{} for JSON marshaling
	queryVectorInterface := make([]interface{}, len(queryVector))
	for i, v := range queryVector {
		queryVectorInterface[i] = v
	}

	// Build knn expression
	k := knn.NResults
	if k <= 0 {
		k = 10
	}

	knnExpr := map[string]interface{}{
		"field":        "embedding",
		"k":            k,
		"query_vector": queryVectorInterface,
	}

	// Add filter if present
	filterConditions := c.buildMetadataFilterForSearchParm(knn.Where)
	if len(filterConditions) > 0 {
		knnExpr["filter"] = filterConditions
	}

	return knnExpr, nil
}

// transformHybridSearchResults transforms SQL query results to HybridSearchResult.
func (c *Client) transformHybridSearchResults(rows *sql.Rows) (*HybridSearchResult, error) {
	result := &HybridSearchResult{
		IDs:        []string{},
		Distances:  []float64{},
		Documents:  []string{},
		Metadatas:  []Metadata{},
		Embeddings: [][]float32{},
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Create a map of column names for lookup
	colMap := make(map[string]int)
	for i, col := range cols {
		colMap[strings.ToLower(col)] = i
	}

	for rows.Next() {
		// Create a slice to hold all column values
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Extract ID
		var id string
		if idx, ok := colMap["id"]; ok {
			id = c.convertToString(values[idx])
		} else if idx, ok := colMap["_id"]; ok {
			id = c.convertToString(values[idx])
		}
		result.IDs = append(result.IDs, id)

		// Extract distance/score
		var distance float64
		if idx, ok := colMap["_distance"]; ok {
			distance = c.convertToFloat64(values[idx])
		} else if idx, ok := colMap["distance"]; ok {
			distance = c.convertToFloat64(values[idx])
		} else if idx, ok := colMap["_score"]; ok {
			distance = c.convertToFloat64(values[idx])
		} else if idx, ok := colMap["score"]; ok {
			distance = c.convertToFloat64(values[idx])
		}
		result.Distances = append(result.Distances, distance)

		// Extract document
		var document string
		if idx, ok := colMap["document"]; ok {
			document = c.convertToString(values[idx])
		}
		result.Documents = append(result.Documents, document)

		// Extract metadata
		var metadata Metadata
		if idx, ok := colMap["metadata"]; ok {
			metadataStr := c.convertToString(values[idx])
			if metadataStr != "" {
				json.Unmarshal([]byte(metadataStr), &metadata)
			}
		}
		if metadata == nil {
			metadata = Metadata{}
		}
		result.Metadatas = append(result.Metadatas, metadata)

		// Extract embedding
		var embedding []float32
		if idx, ok := colMap["embedding"]; ok {
			embStr := c.convertToString(values[idx])
			if embStr != "" {
				json.Unmarshal([]byte(embStr), &embedding)
			}
		}
		result.Embeddings = append(result.Embeddings, embedding)
	}

	return result, nil
}

// convertToString converts an interface{} value to string.
func (c *Client) convertToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// convertToFloat64 converts an interface{} value to float64.
func (c *Client) convertToFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	case []byte:
		var f float64
		fmt.Sscanf(string(val), "%f", &f)
		return f
	default:
		return 0
	}
}

// scanQueryResults scans query results from rows.
func (c *Client) scanQueryResults(rows *sql.Rows) ([]string, []float64, []string, []Metadata, [][]float32, error) {
	var ids []string
	var distances []float64
	var documents []string
	var metadatas []Metadata
	var embeddings [][]float32

	for rows.Next() {
		var id, document, metadataJSON, embeddingJSON string
		var distance float64

		if err := rows.Scan(&id, &document, &metadataJSON, &embeddingJSON, &distance); err != nil {
			return nil, nil, nil, nil, nil, err
		}

		ids = append(ids, id)
		distances = append(distances, distance)
		documents = append(documents, document)

		var metadata Metadata
		metadata.FromJSON(metadataJSON)
		metadatas = append(metadatas, metadata)

		var embedding []float32
		json.Unmarshal([]byte(embeddingJSON), &embedding)
		embeddings = append(embeddings, embedding)
	}

	return ids, distances, documents, metadatas, embeddings, nil
}

// vectorToString converts a float32 slice to a string format for SQL embedding.
// Format: [0.1,0.2,0.3] (matching Python's vector_str format)
func vectorToString(vector []float32) string {
	parts := make([]string, len(vector))
	for i, v := range vector {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
