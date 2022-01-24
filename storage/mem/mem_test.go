package mem

import (
	"testing"

	"github.com/myml/kfs-ks/storage/testutils"
	"github.com/stretchr/testify/require"
)

func TestMemStorage(t *testing.T) {
	assert := require.New(t)
	store := &Storage{}
	testutils.TestStorage(assert, store)
}
