package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	TypeInt64  byte = 1
	TypeString byte = 2
	TypeByte   byte = 3
	TypeBool   byte = 4
	TypeInt32  byte = 5

	TypeCollumnDefinetion = 99
	TypeRecord            = 100
)

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
		return TypeByte, nil
	case int32:
		return TypeInt32, nil
	case int64:
		return TypeInt64, nil

	case string:
		return TypeString, nil
	case bool:
		return TypeBool, nil
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
	err = binary.Write(&buf, binary.LittleEndian, typeFlag)
	if err != nil {
		return nil, err
	}

	// write length
	lengthData, err := t.lengthData()
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, lengthData)
	if err != nil {
		return nil, err
	}
	// write value
	valueBuf, err := t.valueMarshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(valueBuf)

	return buf.Bytes(), nil
}
