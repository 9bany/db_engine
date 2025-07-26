package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	t.Run("TestValueUnmarshaler", func(t *testing.T) {
		value := "test"
		unmarshaler := NewValueUnmarshaler[string]()
		data := []byte(value)
		err := unmarshaler.UnmarshalBinary(data)
		assert.Nil(t, err)
		assert.Equal(t, value, unmarshaler.Value)
	})

	t.Run("TestValueMarshaler", func(t *testing.T) {
		value := "test"
		marshaler := NewValueMarshaler(value)
		data, err := marshaler.MarshalBinary()
		assert.Nil(t, err)
		assert.Equal(t, []byte{116, 101, 115, 116}, data)
	})
}
