package file

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	assert := require.New(t)
	storage := &Storage{}
	data := make([]byte, 1024)
	_, err := io.ReadFull(rand.Reader, data[:])
	assert.NoError(err)
	err = storage.Set("test", bytes.NewReader(data[:]))
	assert.NoError(err)

	r, err := storage.Get("test", 0, 0)
	assert.NoError(err)
	defer r.Close()
	getData, err := io.ReadAll(r)
	assert.NoError(err)
	assert.EqualValues(data, getData)

	r, err = storage.Get("test", 10, 0)
	assert.NoError(err)
	defer r.Close()
	getData, err = io.ReadAll(r)
	assert.NoError(err)
	assert.EqualValues(data[10:], getData)

	r, err = storage.Get("test", 10, 50)
	assert.NoError(err)
	defer r.Close()
	getData, err = io.ReadAll(r)
	assert.NoError(err)
	assert.EqualValues(data[10:10+50], getData)
}
