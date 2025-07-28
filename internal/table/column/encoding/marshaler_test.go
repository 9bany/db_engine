package encoding

import (
	"testing"

	"github.com/9bany/db/internal/platform/types"
	"github.com/stretchr/testify/assert"
)

func TestColumnDefMarshal(t *testing.T) {
	name := "id"
	var colName [64]byte
	copy(colName[:], name)
	marshaler := NewColumnDefinitionMarshaler(colName, types.TypeInt32, false)
	data, err := marshaler.MarshalBinary()
	assert.Nil(t, err)
	assert.NotEmpty(t, data)
	assert.Equal(t, types.TypeColumnDefinition, data[0])
	err = marshaler.UnmarshalBinary(data)
	assert.Nil(t, err)
	assert.Equal(t, colName, marshaler.Name)
	assert.Equal(t, types.TypeInt32, marshaler.DataType)
	assert.False(t, marshaler.AllowNull)
}
