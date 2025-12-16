package goseekdb

import (
	"encoding/json"
)

// DistanceMetric represents the distance metric used for vector similarity.
type DistanceMetric string

const (
	// DistanceL2 represents Euclidean distance.
	DistanceL2 DistanceMetric = "l2"
	// DistanceCosine represents cosine similarity.
	DistanceCosine DistanceMetric = "cosine"
	// DistanceInnerProduct represents inner product.
	DistanceInnerProduct DistanceMetric = "inner_product"
)

// DefaultVectorDimension is the default dimension for embeddings (matches all-MiniLM-L6-v2).
const DefaultVectorDimension = 384

// DefaultDistanceMetric is the default distance metric.
const DefaultDistanceMetric = DistanceCosine

// DistanceFuncName returns the SQL function name for the distance metric.
func (d DistanceMetric) DistanceFuncName() string {
	switch d {
	case DistanceL2:
		return "l2_distance"
	case DistanceCosine:
		return "cosine_distance"
	case DistanceInnerProduct:
		return "inner_product"
	default:
		return "l2_distance" // Default to L2
	}
}

// HNSWConfiguration represents the HNSW index configuration for a collection.
type HNSWConfiguration struct {
	Dimension int            `json:"dimension"`
	Distance  DistanceMetric `json:"distance"`
}

// Database represents a database in SeekDB.
type Database struct {
	Name      string `json:"name"`
	Tenant    string `json:"tenant,omitempty"`
	Charset   string `json:"charset,omitempty"`
	Collation string `json:"collation,omitempty"`
}

// Metadata represents arbitrary JSON metadata for a document.
type Metadata map[string]interface{}

// QueryResult contains the results of a vector search query.
type QueryResult struct {
	IDs        [][]string    `json:"ids"`
	Distances  [][]float64   `json:"distances,omitempty"`
	Documents  [][]string    `json:"documents,omitempty"`
	Metadatas  [][]Metadata  `json:"metadatas,omitempty"`
	Embeddings [][][]float32 `json:"embeddings,omitempty"`
}

// GetResult contains the results of a get operation.
type GetResult struct {
	IDs        []string    `json:"ids"`
	Documents  []string    `json:"documents,omitempty"`
	Metadatas  []Metadata  `json:"metadatas,omitempty"`
	Embeddings [][]float32 `json:"embeddings,omitempty"`
}

// HybridSearchResult contains the results of a hybrid search.
type HybridSearchResult struct {
	IDs        []string    `json:"ids"`
	Distances  []float64   `json:"distances,omitempty"`
	Documents  []string    `json:"documents,omitempty"`
	Metadatas  []Metadata  `json:"metadatas,omitempty"`
	Embeddings [][]float32 `json:"embeddings,omitempty"`
}

// RRFConfig represents configuration for Reciprocal Rank Fusion.
type RRFConfig struct {
	K int `json:"k"` // Constant used in RRF formula: 1/(k + rank)
}

// HybridSearchQuery represents a query for hybrid search.
type HybridSearchQuery struct {
	WhereDocument Filter `json:"where_document,omitempty"`
	Where         Filter `json:"where,omitempty"`
	NResults      int    `json:"n_results"`
}

// HybridSearchKNN represents KNN parameters for hybrid search.
type HybridSearchKNN struct {
	QueryTexts      []string    `json:"query_texts,omitempty"`
	QueryEmbeddings [][]float32 `json:"query_embeddings,omitempty"`
	Where           Filter      `json:"where,omitempty"`
	NResults        int         `json:"n_results"`
}

// HybridSearchRank represents ranking configuration for hybrid search.
type HybridSearchRank struct {
	RRF *RRFConfig `json:"rrf,omitempty"`
}

// Filter represents a filter condition for queries.
// Supports operators like $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $and, $or, $not, $contains, $regex.
type Filter map[string]interface{}

// ToJSON converts metadata to JSON string.
func (m Metadata) ToJSON() (string, error) {
	if m == nil {
		return "{}", nil
	}
	bytes, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// FromJSON parses JSON string into metadata.
func (m *Metadata) FromJSON(s string) error {
	if s == "" || s == "{}" {
		*m = Metadata{}
		return nil
	}
	return json.Unmarshal([]byte(s), m)
}

// CollectionInfo contains metadata about a collection.
type CollectionInfo struct {
	Name      string         `json:"name"`
	Dimension int            `json:"dimension"`
	Distance  DistanceMetric `json:"distance"`
}

// Field names used in collection tables.
const (
	FieldID        = "_id"
	FieldDocument  = "document"
	FieldEmbedding = "embedding"
	FieldMetadata  = "metadata"
)

// TableNamePrefix is the prefix for collection tables.
const TableNamePrefix = "c$v1$"

// GetTableName returns the database table name for a collection.
func GetTableName(collectionName string) string {
	return TableNamePrefix + collectionName
}
