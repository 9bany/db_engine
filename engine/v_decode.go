package engine

import (
	"bytes"
	"encoding/binary"
)

type ValueUnmarshaler[T any] struct {
	value T
}

func (f *ValueUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	switch v := any(&f.value).(type) {
	case *string:
		*v = string(data)
	default:
		if err := binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &f.value); err != nil {
			return err
		}
	}
	return nil
}
