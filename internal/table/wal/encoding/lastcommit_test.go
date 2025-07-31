package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLastCommitMarshaler_EmptyID(t *testing.T) {
	marshaler := NewLastCommitMarshaler("", 12)
	byteData, err := marshaler.MarshalBinary()
	assert.Nil(t, err)
	assert.NotEmpty(t, byteData)
	unmarshaler := NewLastCommitUnmarshaler()
	err = unmarshaler.UnmarshalBinary(byteData)
	assert.Nil(t, err)
	assert.Equal(t, unmarshaler.ID, "")
	assert.Equal(t, unmarshaler.Len, uint32(12))
}

func TestLastCommitMarshaler_ZeroLength(t *testing.T) {
	marshaler := NewLastCommitMarshaler("123", 0)
	byteData, err := marshaler.MarshalBinary()
	assert.Nil(t, err)
	assert.NotEmpty(t, byteData)
	unmarshaler := NewLastCommitUnmarshaler()
	err = unmarshaler.UnmarshalBinary(byteData)
	assert.Nil(t, err)
	assert.Equal(t, unmarshaler.ID, "123")
	assert.Equal(t, unmarshaler.Len, uint32(0))
}

func TestLastCommitUnmarshaler_InvalidData(t *testing.T) {
	unmarshaler := NewLastCommitUnmarshaler()
	err := unmarshaler.UnmarshalBinary([]byte{})
	assert.NotNil(t, err)
}

func TestLastCommitMarshaler_MaxValues(t *testing.T) {
	marshaler := NewLastCommitMarshaler("123", ^uint32(0)) // Max uint32 value
	byteData, err := marshaler.MarshalBinary()
	assert.Nil(t, err)
	assert.NotEmpty(t, byteData)
	unmarshaler := NewLastCommitUnmarshaler()
	err = unmarshaler.UnmarshalBinary(byteData)
	assert.Nil(t, err)
	assert.Equal(t, unmarshaler.ID, "123")
	assert.Equal(t, unmarshaler.Len, ^uint32(0))
}
