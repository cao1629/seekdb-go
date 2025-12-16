package goseekdb

import (
	"time"

	"github.com/ob-labs/seekdb-go/embedding"
)

// ClientOption is a functional option for configuring a Client.
type ClientOption func(*ClientConfig)

// ClientConfig holds the configuration for a Client.
type ClientConfig struct {
	// For embedded mode
	Path string

	// For remote mode
	Host     string
	Port     int
	User     string
	Password string
	Tenant   string

	// Common options
	Database         string
	ConnectTimeout   time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	MaxConnections   int
	EmbeddingFunc    embedding.EmbeddingFunc
	AutoConnect      bool
}

// DefaultClientConfig returns a default client configuration.
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Port:           2881,
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxConnections: 10,
		AutoConnect:    true,
		Tenant:         "test",
	}
}

// WithPath sets the path for embedded mode.
func WithPath(path string) ClientOption {
	return func(c *ClientConfig) {
		c.Path = path
	}
}

// WithHost sets the host for remote mode.
func WithHost(host string) ClientOption {
	return func(c *ClientConfig) {
		c.Host = host
	}
}

// WithPort sets the port for remote mode.
func WithPort(port int) ClientOption {
	return func(c *ClientConfig) {
		c.Port = port
	}
}

// WithUser sets the username for authentication.
func WithUser(user string) ClientOption {
	return func(c *ClientConfig) {
		c.User = user
	}
}

// WithPassword sets the password for authentication.
func WithPassword(password string) ClientOption {
	return func(c *ClientConfig) {
		c.Password = password
	}
}

// WithTenant sets the tenant for multi-tenant systems.
func WithTenant(tenant string) ClientOption {
	return func(c *ClientConfig) {
		c.Tenant = tenant
	}
}

// WithDatabase sets the database name.
func WithDatabase(database string) ClientOption {
	return func(c *ClientConfig) {
		c.Database = database
	}
}

// WithConnectTimeout sets the connection timeout.
func WithConnectTimeout(timeout time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.ConnectTimeout = timeout
	}
}

// WithReadTimeout sets the read timeout.
func WithReadTimeout(timeout time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.ReadTimeout = timeout
	}
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(timeout time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.WriteTimeout = timeout
	}
}

// WithMaxConnections sets the maximum number of connections.
func WithMaxConnections(max int) ClientOption {
	return func(c *ClientConfig) {
		c.MaxConnections = max
	}
}

// WithEmbeddingFunc sets the default embedding function for the client.
func WithEmbeddingFunc(fn embedding.EmbeddingFunc) ClientOption {
	return func(c *ClientConfig) {
		c.EmbeddingFunc = fn
	}
}

// WithAutoConnect enables or disables automatic connection on first operation.
func WithAutoConnect(autoConnect bool) ClientOption {
	return func(c *ClientConfig) {
		c.AutoConnect = autoConnect
	}
}

// CreateCollectionOptions holds options for creating a collection.
type CreateCollectionOptions struct {
	Configuration       *HNSWConfiguration
	EmbeddingFunc       embedding.EmbeddingFunc
	EmbeddingFuncSet    bool // true if embedding function was explicitly set (even to nil)
	GetOrCreate         bool
}

// CreateCollectionOption is a functional option for CreateCollection.
type CreateCollectionOption func(*CreateCollectionOptions)

// WithConfiguration sets the HNSW configuration for the collection.
func WithConfiguration(config *HNSWConfiguration) CreateCollectionOption {
	return func(o *CreateCollectionOptions) {
		o.Configuration = config
	}
}

// WithCollectionEmbeddingFunc sets the embedding function for the collection.
// Pass nil to explicitly disable embedding function (for pre-computed embeddings).
func WithCollectionEmbeddingFunc(fn embedding.EmbeddingFunc) CreateCollectionOption {
	return func(o *CreateCollectionOptions) {
		o.EmbeddingFunc = fn
		o.EmbeddingFuncSet = true
	}
}

// WithGetOrCreate sets whether to get existing collection or create new.
func WithGetOrCreate(getOrCreate bool) CreateCollectionOption {
	return func(o *CreateCollectionOptions) {
		o.GetOrCreate = getOrCreate
	}
}

// AddOptions holds options for adding documents to a collection.
type AddOptions struct {
	Embeddings [][]float32
	Metadatas  []Metadata
}

// AddOption is a functional option for Add operations.
type AddOption func(*AddOptions)

// WithEmbeddings provides pre-computed embeddings.
func WithEmbeddings(embeddings [][]float32) AddOption {
	return func(o *AddOptions) {
		o.Embeddings = embeddings
	}
}

// WithMetadatas provides metadata for documents.
func WithMetadatas(metadatas []Metadata) AddOption {
	return func(o *AddOptions) {
		o.Metadatas = metadatas
	}
}

// QueryOptions holds options for querying a collection.
type QueryOptions struct {
	QueryEmbeddings [][]float32
	Where           Filter
	WhereDocument   Filter
	Include         []string
}

// QueryOption is a functional option for Query operations.
type QueryOption func(*QueryOptions)

// WithQueryEmbeddings provides pre-computed query embeddings.
func WithQueryEmbeddings(embeddings [][]float32) QueryOption {
	return func(o *QueryOptions) {
		o.QueryEmbeddings = embeddings
	}
}

// WithWhere sets metadata filters for the query.
func WithWhere(filter Filter) QueryOption {
	return func(o *QueryOptions) {
		o.Where = filter
	}
}

// WithWhereDocument sets document filters for the query.
func WithWhereDocument(filter Filter) QueryOption {
	return func(o *QueryOptions) {
		o.WhereDocument = filter
	}
}

// WithInclude specifies which fields to include in results.
func WithInclude(fields []string) QueryOption {
	return func(o *QueryOptions) {
		o.Include = fields
	}
}

// GetOptions holds options for getting documents from a collection.
type GetOptions struct {
	Where         Filter
	WhereDocument Filter
	Limit         int
	Offset        int
	Include       []string
}

// GetOption is a functional option for Get operations.
type GetOption func(*GetOptions)

// WithGetWhere sets metadata filters for get operations.
func WithGetWhere(filter Filter) GetOption {
	return func(o *GetOptions) {
		o.Where = filter
	}
}

// WithGetWhereDocument sets document filters for get operations.
func WithGetWhereDocument(filter Filter) GetOption {
	return func(o *GetOptions) {
		o.WhereDocument = filter
	}
}

// WithLimit sets the maximum number of results.
func WithLimit(limit int) GetOption {
	return func(o *GetOptions) {
		o.Limit = limit
	}
}

// WithOffset sets the offset for pagination.
func WithOffset(offset int) GetOption {
	return func(o *GetOptions) {
		o.Offset = offset
	}
}

// WithGetInclude specifies which fields to include in results.
func WithGetInclude(fields []string) GetOption {
	return func(o *GetOptions) {
		o.Include = fields
	}
}

// UpdateOptions holds options for updating documents.
type UpdateOptions struct {
	Documents  []string
	Embeddings [][]float32
	Metadatas  []Metadata
}

// UpdateOption is a functional option for Update operations.
type UpdateOption func(*UpdateOptions)

// WithUpdateDocuments sets documents to update.
func WithUpdateDocuments(documents []string) UpdateOption {
	return func(o *UpdateOptions) {
		o.Documents = documents
	}
}

// WithUpdateEmbeddings sets embeddings to update.
func WithUpdateEmbeddings(embeddings [][]float32) UpdateOption {
	return func(o *UpdateOptions) {
		o.Embeddings = embeddings
	}
}

// WithUpdateMetadatas sets metadata to update.
func WithUpdateMetadatas(metadatas []Metadata) UpdateOption {
	return func(o *UpdateOptions) {
		o.Metadatas = metadatas
	}
}
