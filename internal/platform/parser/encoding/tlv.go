package encoding

import (
	"bytes"

	"github.com/9bany/db/internal/platform/types"
)

type TLVUnmarshaler[T any] struct {
	dataType    byte
	length      uint32
	Value       T
	unmarshaler *ValueUnmarshaler[T]
	BytesRead   uint32
}

func NewTLVUnmarshaler[T any](u *ValueUnmarshaler[T]) *TLVUnmarshaler[T] {
	return &TLVUnmarshaler[T]{
		unmarshaler: u,
	}
}

func (t *TLVUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	typeUnmarshal := &ValueUnmarshaler[byte]{}
	err := typeUnmarshal.UnmarshalBinary(data)
	if err != nil {
		return err
	}
	t.dataType = typeUnmarshal.Value

	t.BytesRead += 1

	lengthUnmarshal := &ValueUnmarshaler[uint32]{}
	err = lengthUnmarshal.UnmarshalBinary(data[t.BytesRead:])
	if err != nil {
		return err
	}
	t.length = lengthUnmarshal.Value
	t.BytesRead += 4

	err = t.unmarshaler.UnmarshalBinary(data[t.BytesRead:])
	if err != nil {

	}
	t.Value = t.unmarshaler.Value
	t.BytesRead += t.length
	return nil
}

func NewTLVMarshaler[T any](value T) *TLVMarshaler[T] {
	return &TLVMarshaler[T]{
		value:          value,
		valueMarshaler: &ValueMarshaler[T]{value: value},
	}
}

type TLVMarshaler[T any] struct {
	value          T
	valueMarshaler *ValueMarshaler[T]
}

func (t *TLVMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	// write type
	typeFlag, err := types.TypeBytes(t.value)
	if err != nil {
		return nil, err
	}
	typeMarshaler := ValueMarshaler[byte]{value: typeFlag}
	typeBuf, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(typeBuf)

	// write length
	lengthData, err := types.LengthData(t.value)
	if err != nil {
		return nil, err
	}
	lengthMarshaler := ValueMarshaler[uint32]{value: lengthData}
	lengthBuf, err := lengthMarshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(lengthBuf)

	// write value
	valueBuf, err := t.valueMarshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(valueBuf)

	return buf.Bytes(), nil
}

func (t *TLVMarshaler[T]) TLVLength() (uint32, error) {
	length, err := types.LengthData(t.value)
	if err != nil {
		return 0, err
	}
	return types.LenByte + types.LenInt32 + length, nil
}
