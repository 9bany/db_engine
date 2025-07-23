package encoding

import (
	"bytes"
	"fmt"

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

type UnsupportedDataTypeError struct {
	dataType string
}

func (u *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("unsupported data type %s", u.dataType)
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

func (t *TLVMarshaler[T]) typeBytes() (byte, error) {
	switch v := any(t.value).(type) {
	case byte:
		return types.TypeByte, nil
	case int32:
		return types.TypeInt32, nil
	case int64:
		return types.TypeInt64, nil
	case string:
		return types.TypeString, nil
	case bool:
		return types.TypeBool, nil
	default:
		return 0, &UnsupportedDataTypeError{dataType: fmt.Sprint(v)}
	}
}

func (t *TLVMarshaler[T]) lengthData() (uint32, error) {
	switch v := any(t.value).(type) {
	case byte:
		return 1, nil
	case int32:
		return 4, nil
	case int64:
		return 8, nil
	case string:
		return uint32(len(v)), nil
	case bool:
		return 1, nil
	default:
		return 0, &UnsupportedDataTypeError{dataType: fmt.Sprint(v)}
	}
}

func (t *TLVMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	// write type
	typeFlag, err := t.typeBytes()
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
	lengthData, err := t.lengthData()
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
