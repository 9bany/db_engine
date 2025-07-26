package column

import (
	"testing"

	"github.com/9bany/db/internal/platform/types"
	"github.com/stretchr/testify/assert"
)

func TestColumn(t *testing.T) {
	id := NewColumn("id", types.TypeInt64, ColumnOptions{Nullable: false})
	b, err := id.MarshalBinary()
	assert.Nil(t, err)
	assert.Equal(t, 86, len(b))
}
