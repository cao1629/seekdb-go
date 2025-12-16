# seekdb-go

A Go client library for seekdb

## Features

- **Vector Similarity Search** - Search documents using vector embeddings
- **Hybrid Search** - Combine vector and full-text search with Reciprocal Rank Fusion (RRF)
- **Automatic Embeddings** - Built-in embedding generation using ONNX Runtime (all-MiniLM-L6-v2 model)
- **Custom Embeddings** - Support for custom embedding functions

## Installation

```bash
go get github.com/ob-labs/seekdb-go
```

## Deployment Modes

seekdb-go supports two deployment modes:

### Server-Client Mode

In this mode, the client connects to a remote seekdb server over the network. This is the recommended mode for production deployments.

```go
client, err := goseekdb.NewClient(
    goseekdb.WithHost("127.0.0.1"),
    goseekdb.WithPort(2881),
    goseekdb.WithDatabase("test"),
)
```

### Embedded Mode

In embedded mode, the database runs within the same process as your application. This is useful for development, testing, or single-node deployments.

**Note:** seekdb-go doesn't support embedded mode yet.

## Quick Start

### Create a Client

```go
package main

import (
    "context"
    "log"

    "github.com/ob-labs/seekdb-go"
)

func main() {
    ctx := context.Background()

    // Create a client connected to seekdb
    client, err := goseekdb.NewClient(
        goseekdb.WithHost("127.0.0.1"),
        goseekdb.WithPort(2881),
        goseekdb.WithDatabase("test"),
        goseekdb.WithUsername("root"),
        goseekdb.WithPassword(""),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create a collection
    collection, err := client.CreateCollection(ctx, "my_documents")
    if err != nil {
        log.Fatal(err)
    }

    // Add documents (embeddings are auto-generated)
    ids := []string{"doc1", "doc2", "doc3"}
    documents := []string{
        "The quick brown fox jumps over the lazy dog",
        "Machine learning is transforming industries",
        "Vector databases enable semantic search",
    }

    err = collection.Add(ctx, ids, documents)
    if err != nil {
        log.Fatal(err)
    }

    // Query for similar documents
    results, err := collection.Query(ctx, []string{"artificial intelligence"}, 2)
    if err != nil {
        log.Fatal(err)
    }

    // Print results
    for i, id := range results.IDs[0] {
        log.Printf("ID: %s, Distance: %f, Document: %s\n",
            id, results.Distances[0][i], results.Documents[0][i])
    }

    // Cleanup
    client.DeleteCollection(ctx, "my_documents")
}
```

### Add Documents with Metadata

```go
ids := []string{"doc1", "doc2"}
documents := []string{
    "Introduction to Go programming",
    "Advanced Python techniques",
}
metadatas := []map[string]any{
    {"language": "go", "level": "beginner"},
    {"language": "python", "level": "advanced"},
}

err = collection.Add(ctx, ids, documents,
    goseekdb.WithMetadatas(metadatas),
)
```

### Query with Filters

```go
// Filter by metadata
results, err := collection.Query(ctx, []string{"programming tutorial"}, 10,
    goseekdb.WithWhereFilter(map[string]any{
        "language": map[string]any{"$eq": "go"},
    }),
)

// Filter by document content
results, err := collection.Query(ctx, []string{"programming"}, 10,
    goseekdb.WithWhereDocumentFilter(map[string]any{
        "$contains": "introduction",
    }),
)
```

### Hybrid Search

Combine vector similarity and full-text search for better results:

```go
results, err := collection.HybridSearch(ctx,
    []string{"machine learning basics"},  // query texts
    10,                                    // number of results
    goseekdb.WithFullTextWeight(0.3),     // weight for full-text search
    goseekdb.WithVectorWeight(0.7),       // weight for vector search
)
```

### Use Pre-computed Embeddings

```go
ids := []string{"doc1"}
embeddings := [][]float32{{0.1, 0.2, 0.3, ...}} // 384-dimensional vectors
documents := []string{"My document text"}

err = collection.Add(ctx, ids, documents,
    goseekdb.WithEmbeddings(embeddings),
)
```

## Configuration Options

### Client Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithHost(host)` | Database host | `"127.0.0.1"` |
| `WithPort(port)` | Database port | `2881` |
| `WithDatabase(db)` | Database name | `"test"` |
| `WithUsername(user)` | Database username | `"root"` |
| `WithPassword(pass)` | Database password | `""` |
| `WithTenant(tenant)` | OceanBase tenant | `""` |
| `WithEmbeddingFunc(fn)` | Custom embedding function | Default ONNX |

### Collection Options

| Option | Description |
|--------|-------------|
| `WithMetadatas(m)` | Add metadata to documents |
| `WithEmbeddings(e)` | Use pre-computed embeddings |
| `WithWhereFilter(f)` | Filter by metadata |
| `WithWhereDocumentFilter(f)` | Filter by document content |
| `WithInclude(fields...)` | Specify fields to return |

### Filter Operators

**Metadata Filters:**
- `$eq`, `$ne` - Equal / Not equal
- `$gt`, `$gte`, `$lt`, `$lte` - Comparisons
- `$in`, `$nin` - In / Not in list
- `$and`, `$or`, `$not` - Logical operators

**Document Filters:**
- `$contains` - Full-text search
- `$regex` - Regular expression match
- `$and`, `$or`, `$not` - Logical operators

## Database Administration

```go
// Create an admin client
adminClient := client.Admin()

// Create a new database
err := adminClient.CreateDatabase(ctx, "new_database")

// List all databases
databases, err := adminClient.ListDatabases(ctx)

// Delete a database
err := adminClient.DeleteDatabase(ctx, "old_database")
```



