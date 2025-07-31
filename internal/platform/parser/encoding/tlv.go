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

func (u *TLVUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	u.BytesRead = 0

	byteUnmarshaler := NewValueUnmarshaler[byte]()
	intUnmarshaler := NewValueUnmarshaler[uint32]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.dataType = byteUnmarshaler.Value
	u.BytesRead += types.LenByte

	// length
	if err := intUnmarshaler.UnmarshalBinary(data[u.BytesRead:]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.length = intUnmarshaler.Value
	u.BytesRead += types.LenInt32

	// value
	if err := u.unmarshaler.UnmarshalBinary(data[u.BytesRead:(u.BytesRead + u.length)]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.Value = u.unmarshaler.Value
	u.BytesRead += u.length

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

func (m *TLVMarshaler[T]) TLVLength() (uint32, error) {
	switch v := any(m.value).(type) {
	case byte:
		return types.LenMeta + types.LenByte, nil
	case int32, uint32:
		return types.LenMeta + types.LenInt32, nil
	case int64:
		return types.LenMeta + types.LenInt64, nil
	case bool:
		return types.LenMeta + types.LenByte, nil
	case string:
		return types.LenMeta + uint32(len(v)), nil
	default:
		return 0, &UnsupportedDataTypeError{dataType: fmt.Sprintf("%T", v)}
	}
}
