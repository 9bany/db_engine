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
	unmarshaler := NewColumnDefinitionUnmarshaler(colName, types.TypeInt32, false)
	err = unmarshaler.UnmarshalBinary(data)
	assert.Nil(t, err)
	assert.Equal(t, colName, unmarshaler.Name)
	assert.Equal(t, types.TypeInt32, unmarshaler.DataType)
	assert.False(t, unmarshaler.AllowNull)
}
