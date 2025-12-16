package embedding

import (
	"fmt"
	"sync"
)

// EmbeddingFunc is a function that converts text to embeddings.
// It takes either a single string or a slice of strings and returns embeddings.
type EmbeddingFunc interface {

	// Embed converts text to embedding vectors.
	// Input can be a single string or multiple strings.
	// Returns a slice of embedding vectors (each vector is []float32).
	Embed(texts []string) ([][]float32, error)

	// Dimension returns the dimension of the embeddings produced by this function.
	Dimension() int
}

var (
	defaultEmbeddingFunc EmbeddingFunc
	defaultOnce          sync.Once
	defaultErr           error
)

// DefaultEmbeddingFunc returns the default embedding function.
// This uses the all-MiniLM-L6-v2 ONNX model producing 384-dimensional embeddings.
func DefaultEmbeddingFunc() (EmbeddingFunc, error) {
	defaultOnce.Do(func() {
		// Try to create ONNX embedding function
		ef, err := NewONNXEmbeddingFunction()
		if err != nil {
			defaultErr = fmt.Errorf("failed to initialize ONNX embedding function: %w\n"+
				"Note: ONNX Runtime library is required. Install from: https://github.com/microsoft/onnxruntime/releases\n"+
				"Alternatively, use a custom embedding function or pre-computed embeddings", err)
			return
		}
		defaultEmbeddingFunc = ef
	})

	if defaultErr != nil {
		return nil, defaultErr
	}

	return defaultEmbeddingFunc, nil
}
