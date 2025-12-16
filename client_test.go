package goseekdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = require.Equal // Suppress unused import warning

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		opts    []ClientOption
		wantErr bool
	}{
		{
			name: "remote client with valid config",
			opts: []ClientOption{
				WithHost("localhost"),
				WithPort(2881),
				WithDatabase("test"),
				WithUser("root"),
				WithAutoConnect(false), // Don't actually connect in tests
			},
			wantErr: false,
		},
		{
			name: "embedded client with path",
			opts: []ClientOption{
				WithPath("/tmp/seekdb"),
				WithDatabase("test"),
				WithAutoConnect(false),
			},
			wantErr: false,
		},
		{
			name:    "no config should fail",
			opts:    []ClientOption{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.opts...)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				if client != nil {
					client.Close()
				}
			}
		})
	}
}

func TestClientConfig(t *testing.T) {
	config := DefaultClientConfig()

	assert.Equal(t, 2881, config.Port)
	assert.Equal(t, "test", config.Tenant)
	assert.True(t, config.AutoConnect)
	assert.NotZero(t, config.ConnectTimeout)
}

func TestClientOptions(t *testing.T) {
	config := DefaultClientConfig()

	WithHost("example.com")(config)
	assert.Equal(t, "example.com", config.Host)

	WithPort(3306)(config)
	assert.Equal(t, 3306, config.Port)

	WithDatabase("mydb")(config)
	assert.Equal(t, "mydb", config.Database)

	WithUser("admin")(config)
	assert.Equal(t, "admin", config.User)

	WithPassword("secret")(config)
	assert.Equal(t, "secret", config.Password)

	WithTenant("prod")(config)
	assert.Equal(t, "prod", config.Tenant)
}

func TestGetTableName(t *testing.T) {
	tests := []struct {
		collection string
		want       string
	}{
		{"test", "c$v1$test"},
		{"my_collection", "c$v1$my_collection"},
		{"", "c$v1$"},
	}

	for _, tt := range tests {
		t.Run(tt.collection, func(t *testing.T) {
			got := GetTableName(tt.collection)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMetadata(t *testing.T) {
	metadata := Metadata{
		"name":  "test",
		"count": 42,
		"tags":  []string{"a", "b"},
	}

	// Test ToJSON
	jsonStr, err := metadata.ToJSON()
	require.NoError(t, err)
	assert.Contains(t, jsonStr, "name")
	assert.Contains(t, jsonStr, "test")

	// Test FromJSON
	var newMetadata Metadata
	err = newMetadata.FromJSON(jsonStr)
	require.NoError(t, err)
	assert.Equal(t, "test", newMetadata["name"])
	assert.Equal(t, float64(42), newMetadata["count"]) // JSON numbers are float64

	// Test empty metadata
	empty := Metadata{}
	jsonStr, err = empty.ToJSON()
	require.NoError(t, err)
	assert.Equal(t, "{}", jsonStr)
}

func TestHNSWConfiguration(t *testing.T) {
	config := &HNSWConfiguration{
		Dimension: 384,
		Distance:  DistanceCosine,
	}

	assert.Equal(t, 384, config.Dimension)
	assert.Equal(t, DistanceCosine, config.Distance)
}

func TestDistanceMetrics(t *testing.T) {
	assert.Equal(t, DistanceMetric("l2"), DistanceL2)
	assert.Equal(t, DistanceMetric("cosine"), DistanceCosine)
	assert.Equal(t, DistanceMetric("inner_product"), DistanceInnerProduct)
}

// Integration tests would go here
// These would require an actual SeekDB instance running

/*
func TestIntegration_BasicOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	client, err := NewClient(
		WithHost("localhost"),
		WithPort(2881),
		WithDatabase("test"),
		WithUser("root"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create collection
	collection, err := client.CreateCollection(ctx, "test_collection")
	require.NoError(t, err)

	// Add documents
	err = collection.Add(ctx,
		[]string{"id1"},
		[]string{"test document"},
	)
	require.NoError(t, err)

	// Count
	count, err := collection.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Cleanup
	err = client.DeleteCollection(ctx, "test_collection")
	require.NoError(t, err)
}
*/
