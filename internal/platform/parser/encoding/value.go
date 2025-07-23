package encoding

import (
	"bytes"
	"encoding/binary"
)

type ValueUnmarshaler[T any] struct {
	Value T
}

func NewValueUnmarshaler[T any]() *ValueUnmarshaler[T] {
	return &ValueUnmarshaler[T]{}
}

func (f *ValueUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	switch v := any(&f.Value).(type) {
	case *string:
		*v = string(data)
	default:
		if err := binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &f.Value); err != nil {
			return err
		}
	}
	return nil
}

func NewValueMarshaler[T any](value T) *ValueMarshaler[T] {
	return &ValueMarshaler[T]{value: value}
}

type ValueMarshaler[T any] struct {
	value T
}

func (f *ValueMarshaler[T]) MarshalBinary() (data []byte, err error) {
	buffer := bytes.Buffer{}
	switch v := any(f.value).(type) {
	case string:
		if err := binary.Write(&buffer, binary.LittleEndian, []byte(v)); err != nil {
			return []byte{}, err
		}
	default:
		if err := binary.Write(&buffer, binary.LittleEndian, v); err != nil {
			return []byte{}, err
		}
	}
	return buffer.Bytes(), nil
}
