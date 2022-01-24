package testutils

import (
	"bytes"
	"crypto/rand"
	"io"

	"github.com/myml/kfs-ks/storage"
	"github.com/stretchr/testify/require"
)

func TestStorage(assert *require.Assertions, store storage.Storage) {
	r, err := store.Get("not_exists", 0, 0)
	assert.NoError(err)
	assert.Nil(r)
	r, w := io.Pipe()
	w.CloseWithError(io.ErrUnexpectedEOF)
	err = store.Set("test", r)
	assert.Error(err)

	data := make([]byte, 1024)
	_, err = io.ReadFull(rand.Reader, data[:])
	assert.NoError(err)
	err = store.Set("test", bytes.NewReader(data[:]))
	assert.NoError(err)

	r, err = store.Get("test", 0, 0)
	assert.NoError(err)
	defer r.Close()
	getData, err := io.ReadAll(r)
	assert.NoError(err)
	assert.EqualValues(data, getData)

	r, err = store.Get("test", 10, 0)
	assert.NoError(err)
	defer r.Close()
	getData, err = io.ReadAll(r)
	assert.NoError(err)
	assert.EqualValues(data[10:], getData)

	r, err = store.Get("test", 10, 50)
	assert.NoError(err)
	defer r.Close()
	getData, err = io.ReadAll(r)
	assert.NoError(err)
	assert.EqualValues(data[10:10+50], getData)
}
