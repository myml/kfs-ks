package gz

import (
	"testing"

	"github.com/myml/kfs-ks/storage/mem"
	"github.com/myml/kfs-ks/storage/testutils"
	"github.com/stretchr/testify/require"
)

func TestGZStorage(t *testing.T) {
	assert := require.New(t)
	store := &Storage{RawStorage: &mem.Storage{}}
	testutils.TestStorage(assert, store)
}
