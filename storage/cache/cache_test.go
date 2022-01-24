package cache

import (
	"io"
	"strings"
	"testing"

	"github.com/myml/kfs-ks/storage/mem"
	"github.com/myml/kfs-ks/storage/testutils"
	"github.com/stretchr/testify/require"
)

func TestCacheStorage(t *testing.T) {
	assert := require.New(t)
	store := &Storage{RawStorage: &mem.Storage{}}
	testutils.TestStorage(assert, store)
}

func TestCacheStorageFlush(t *testing.T) {
	assert := require.New(t)
	store := &Storage{RawStorage: &mem.Storage{}}
	store.RawStorage.Set("test", strings.NewReader("test"))
	r, err := store.Get("test", 0, 0)
	assert.NoError(err)
	data, err := io.ReadAll(r)
	assert.NoError(err)
	assert.Equal(string(data), "test")
	err = store.Flush()
	assert.NoError(err)
	r, err = store.RawStorage.Get("test", 0, 0)
	assert.NoError(err)
	data, err = io.ReadAll(r)
	assert.NoError(err)
	assert.Equal(string(data), "test")
}
