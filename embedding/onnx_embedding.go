package embedding

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/sugarme/tokenizer"
	"github.com/sugarme/tokenizer/pretrained"
	ort "github.com/yalue/onnxruntime_go"
)

const (
	// ModelName is the default model name
	ModelName = "all-MiniLM-L6-v2"
	// HFModelID is the Hugging Face model identifier
	HFModelID = "sentence-transformers/all-MiniLM-L6-v2"
	// Dimension is the embedding dimension for all-MiniLM-L6-v2
	Dimension = 384
	// MaxTokens is the maximum sequence length
	MaxTokens = 256
)

// ONNXEmbeddingFunction implements EmbeddingFunc using ONNX Runtime.
type ONNXEmbeddingFunction struct {
	modelPath string
	tokenizer *tokenizer.Tokenizer
	mu        sync.Mutex
	once      sync.Once
	initErr   error
}

// NewONNXEmbeddingFunction creates a new ONNX-based embedding function.
// It automatically downloads the model if not cached.
func NewONNXEmbeddingFunction() (*ONNXEmbeddingFunction, error) {
	// Get cache directory
	cacheDir, err := getCacheDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get cache directory: %w", err)
	}

	modelDir := filepath.Join(cacheDir, "onnx_models", ModelName, "onnx")

	ef := &ONNXEmbeddingFunction{
		modelPath: filepath.Join(modelDir, "model.onnx"),
	}

	// Download model if needed
	if err := ef.downloadModelIfNeeded(modelDir); err != nil {
		return nil, fmt.Errorf("failed to download model: %w", err)
	}

	// Initialize ONNX runtime (lazy)
	// Actual initialization happens on first Embed call

	return ef, nil
}

// getCacheDir returns the cache directory path
func getCacheDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".cache", "goseekdb"), nil
}

// downloadModelIfNeeded downloads the model files if they don't exist
func (e *ONNXEmbeddingFunction) downloadModelIfNeeded(modelDir string) error {
	// Check if model files exist
	requiredFiles := []string{
		"model.onnx",
		"tokenizer.json",
	}

	allExist := true
	for _, file := range requiredFiles {
		if _, err := os.Stat(filepath.Join(modelDir, file)); os.IsNotExist(err) {
			allExist = false
			break
		}
	}

	if allExist {
		return nil // All files already downloaded
	}

	// Create directory
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		return fmt.Errorf("failed to create model directory: %w", err)
	}

	// Get HF endpoint (support mirrors)
	hfEndpoint := os.Getenv("HF_ENDPOINT")
	if hfEndpoint == "" {
		//hfEndpoint = "https://huggingface.co"
		hfEndpoint = "https://hf-mirror.com"
	}

	fmt.Printf("Downloading model from %s...\n", hfEndpoint)

	// Files to download (HF path -> local filename)
	filesToDownload := map[string]string{
		"onnx/model.onnx": "model.onnx",
		"tokenizer.json":  "tokenizer.json",
	}

	for hfPath, localFile := range filesToDownload {
		localPath := filepath.Join(modelDir, localFile)

		// Skip if already exists
		if _, err := os.Stat(localPath); err == nil {
			continue
		}

		url := fmt.Sprintf("%s/%s/resolve/main/%s", hfEndpoint, HFModelID, hfPath)

		fmt.Printf("Downloading %s...\n", localFile)
		if err := downloadFile(url, localPath); err != nil {
			return fmt.Errorf("failed to download %s: %w", localFile, err)
		}
	}

	fmt.Println("Model downloaded successfully!")
	return nil
}

// downloadFile downloads a file from URL to the destination path
func downloadFile(url, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

var (
	ortInitOnce sync.Once
	ortInitErr  error
)

// getOnnxLibraryPath returns the path to the bundled ONNX Runtime library
func getOnnxLibraryPath() (string, error) {
	// Check in go.mod cache for the bundled library
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get home directory: %w", err)
		}
		gopath = filepath.Join(home, "go")
	}

	// Build path to test_data directory in the module
	// github.com/yalue/onnxruntime_go@v1.24.0/test_data/
	modCache := filepath.Join(gopath, "pkg", "mod", "github.com", "yalue")

	// Find the onnxruntime_go directory
	entries, err := os.ReadDir(modCache)
	if err != nil {
		return "", fmt.Errorf("failed to read mod cache: %w", err)
	}

	// Determine which library to use based on platform
	var libName string
	switch runtime.GOOS {
	case "darwin":
		if runtime.GOARCH == "arm64" {
			libName = "onnxruntime_arm64.dylib"
		} else {
			libName = "onnxruntime_amd64.dylib"
		}
	case "linux":
		if runtime.GOARCH == "arm64" {
			libName = "onnxruntime_arm64.so"
		} else {
			libName = "onnxruntime.so"
		}
	case "windows":
		libName = "onnxruntime.dll"
	default:
		return "", fmt.Errorf("unsupported platform: %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	// Look for the latest version
	var libPath string
	var latestVersion string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Look for onnxruntime_go@v1.24.0 or similar
		if len(name) > 15 && name[:15] == "onnxruntime_go@" {
			testDataDir := filepath.Join(modCache, name, "test_data")
			candidatePath := filepath.Join(testDataDir, libName)

			if _, err := os.Stat(candidatePath); err == nil {
				// Prefer higher version numbers
				if latestVersion == "" || name > latestVersion {
					latestVersion = name
					libPath = candidatePath
				}
			}
		}
	}

	if libPath == "" {
		return "", fmt.Errorf("bundled ONNX Runtime library not found in module cache")
	}

	return libPath, nil
}

// initORT initializes the ONNX session (called once)
func (e *ONNXEmbeddingFunction) initORT() error {
	// Initialize ONNX Runtime globally (once for entire process)
	ortInitOnce.Do(func() {
		// Try to use bundled library first
		if libPath, err := getOnnxLibraryPath(); err == nil {
			ort.SetSharedLibraryPath(libPath)
		}
		// If bundled library not found, try system library
		ortInitErr = ort.InitializeEnvironment()
	})
	if ortInitErr != nil {
		return fmt.Errorf("failed to initialize ONNX runtime: %w", ortInitErr)
	}

	e.once.Do(func() {
		// Load tokenizer
		modelDir := filepath.Dir(e.modelPath)
		tokenizerPath := filepath.Join(modelDir, "tokenizer.json")

		tk, err := pretrained.FromFile(tokenizerPath)
		if err != nil {
			e.initErr = fmt.Errorf("failed to load tokenizer: %w", err)
			return
		}

		// Configure truncation and padding to match Python implementation (max_length=256)
		// This ensures consistent embedding dimensions across Python and Go
		tk.WithTruncation(&tokenizer.TruncationParams{
			MaxLength: MaxTokens,
			Strategy:  tokenizer.LongestFirst,
			Stride:    0,
		})
		tk.WithPadding(&tokenizer.PaddingParams{
			Strategy:  *tokenizer.NewPaddingStrategy(tokenizer.WithFixed(MaxTokens)),
			Direction: tokenizer.Right,
			PadId:     0,
			PadTypeId: 0,
			PadToken:  "[PAD]",
		})

		e.tokenizer = tk
	})

	return e.initErr
}

// DefaultBatchSize is the default batch size for embedding
const DefaultBatchSize = 32

// Embed converts texts to embedding vectors.
// It processes texts in batches of DefaultBatchSize (250) for efficiency.
func (e *ONNXEmbeddingFunction) Embed(texts []string) ([][]float32, error) {
	return e.EmbedWithBatchSize(texts, DefaultBatchSize)
}

// EmbedWithBatchSize converts texts to embedding vectors with a custom batch size.
func (e *ONNXEmbeddingFunction) EmbedWithBatchSize(texts []string, batchSize int) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Initialize ONNX runtime on the first Embed call
	if err := e.initORT(); err != nil {
		return nil, err
	}

	// Process in batches
	allEmbeddings := make([][]float32, 0, len(texts))

	for i := 0; i < len(texts); i += batchSize {
		end := i + batchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[i:end]

		batchEmbeddings, err := e.embedBatch(batch)
		if err != nil {
			return nil, fmt.Errorf("failed to embed batch starting at index %d: %w", i, err)
		}

		allEmbeddings = append(allEmbeddings, batchEmbeddings...)
	}

	return allEmbeddings, nil
}

// embedBatch processes a single batch of texts
func (e *ONNXEmbeddingFunction) embedBatch(texts []string) ([][]float32, error) {
	// Tokenize all texts - truncation and padding are handled by tokenizer config
	encodings := make([]*tokenizer.Encoding, len(texts))
	for i, text := range texts {
		enc, err := e.tokenizer.EncodeSingle(text, true) // true = add special tokens
		if err != nil {
			return nil, fmt.Errorf("failed to encode text %d: %w", i, err)
		}
		encodings[i] = enc
	}

	// Prepare input data - use fixed MaxTokens (256) to match Python implementation
	batchLen := int64(len(texts))
	seqLength := int64(MaxTokens)

	// ONNX runtime Go bindings require flat 1D slices.
	// A 2D Go slice is a slice of pointers to separate allocations - non-contiguous memory.
	// ONNX runtime expects a single contiguous block of memory.
	inputIDs := make([]int64, batchLen*seqLength)
	attentionMask := make([]int64, batchLen*seqLength)
	tokenTypeIDs := make([]int64, batchLen*seqLength)

	for i, enc := range encodings {
		ids := enc.GetIds()
		mask := enc.GetAttentionMask()
		typeIds := enc.GetTypeIds()

		// Copy tokens - tokenizer already handles truncation/padding to MaxTokens
		for j := 0; j < MaxTokens && j < len(ids); j++ {
			offset := i*MaxTokens + j
			inputIDs[offset] = int64(ids[j])
			attentionMask[offset] = int64(mask[j])
			tokenTypeIDs[offset] = int64(typeIds[j])
		}
		// Padding is already zeros from make()
	}

	// Create input tensors
	inputShape := ort.NewShape(batchLen, seqLength)

	inputIDsTensor, err := ort.NewTensor(inputShape, inputIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to create input_ids tensor: %w", err)
	}
	defer inputIDsTensor.Destroy()

	attentionMaskTensor, err := ort.NewTensor(inputShape, attentionMask)
	if err != nil {
		return nil, fmt.Errorf("failed to create attention_mask tensor: %w", err)
	}
	defer attentionMaskTensor.Destroy()

	tokenTypeIDsTensor, err := ort.NewTensor(inputShape, tokenTypeIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to create token_type_ids tensor: %w", err)
	}
	defer tokenTypeIDsTensor.Destroy()

	// Create output tensor
	outputShape := ort.NewShape(batchLen, seqLength, int64(Dimension))
	outputTensor, err := ort.NewEmptyTensor[float32](outputShape)
	if err != nil {
		return nil, fmt.Errorf("failed to create output tensor: %w", err)
	}
	defer outputTensor.Destroy()

	// Create session with pre-allocated tensors
	session, err := ort.NewAdvancedSession(
		e.modelPath,
		[]string{"input_ids", "attention_mask", "token_type_ids"},
		[]string{"last_hidden_state"},
		[]ort.Value{inputIDsTensor, attentionMaskTensor, tokenTypeIDsTensor},
		[]ort.Value{outputTensor},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ONNX session: %w", err)
	}
	defer session.Destroy()

	// Run inference
	if err := session.Run(); err != nil {
		return nil, fmt.Errorf("failed to run inference: %w", err)
	}

	// Get output data
	lastHiddenState := outputTensor.GetData()

	// Apply mean pooling with L2 normalization
	embeddings := meanPooling(lastHiddenState, attentionMask, int(batchLen), int(seqLength), Dimension)

	return embeddings, nil
}

// meanPooling applies mean pooling over token embeddings (matching Python implementation).
func meanPooling(lastHiddenState []float32, attentionMask []int64, batchSize, seqLength, hiddenSize int) [][]float32 {
	embeddings := make([][]float32, batchSize)

	for i := 0; i < batchSize; i++ {
		embedding := make([]float32, hiddenSize)
		maskSum := float32(0)

		// Sum embeddings for non-masked tokens
		for j := 0; j < seqLength; j++ {
			mask := float32(attentionMask[i*seqLength+j])
			maskSum += mask

			for k := 0; k < hiddenSize; k++ {
				idx := i*seqLength*hiddenSize + j*hiddenSize + k
				embedding[k] += lastHiddenState[idx] * mask
			}
		}

		// Average by dividing by number of non-masked tokens (clipped to min 1e-9)
		if maskSum < 1e-9 {
			maskSum = 1e-9
		}
		for k := 0; k < hiddenSize; k++ {
			embedding[k] /= maskSum
		}

		embeddings[i] = embedding
	}

	return embeddings
}

// Dimension returns the embedding dimension
func (e *ONNXEmbeddingFunction) Dimension() int {
	return Dimension
}

// Close cleans up resources
func (e *ONNXEmbeddingFunction) Close() error {
	// Session is created and destroyed per inference call
	// No cleanup needed
	return nil
}
