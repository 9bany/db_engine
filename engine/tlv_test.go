package engine

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	marshaler := NewTLVMarshaler("hello bany")

	data, err := marshaler.MarshalBinary()
	log.Println(data)
	assert.Nil(t, err)
	assert.Equal(t, []byte{2, 10, 0, 0, 0, 104, 101, 108, 108, 111, 32, 98, 97, 110, 121}, data)
}

func TestDecode(t *testing.T) {
	byteData := []byte{2, 10, 0, 0, 0, 104, 101, 108, 108, 111, 32, 98, 97, 110, 121}

	unmarshal := NewTLVUnmarshaler[string]()
	unmarshal.UnmarshalBinary(byteData)

	assert.Equal(t, "hello bany", unmarshal.Value)
}
