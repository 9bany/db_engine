package engine

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
