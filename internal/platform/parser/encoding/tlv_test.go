package encoding

import (
	"log"
	"testing"

	"github.com/9bany/db/internal/platform/types"
	"github.com/stretchr/testify/assert"
)

func TestTLV(t *testing.T) {
	t.Run("TestTLVMarshaler", func(t *testing.T) {
		value := int32(42)
		marshaler := NewTLVMarshaler(value)
		data, err := marshaler.MarshalBinary()
		assert.Nil(t, err)
		assert.Equal(t, types.TypeInt32, data[0])
		log.Println(data)
		assert.Equal(t, []byte{5, 4, 0, 0, 0, 42, 0, 0, 0}, data)
	})

	t.Run("TestTLVUnmarshaler", func(t *testing.T) {
		value := int32(42)
		marshaler := NewTLVMarshaler(value)
		data, _ := marshaler.MarshalBinary()

		unmarshaler := NewTLVUnmarshaler(&ValueUnmarshaler[int32]{})
		err := unmarshaler.UnmarshalBinary(data)
		assert.Nil(t, err)
		assert.Equal(t, value, unmarshaler.Value)
	})
}
