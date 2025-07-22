package engine

import (
	"bytes"
	"encoding/binary"
)

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
