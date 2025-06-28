package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
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

func (f *ValueMarshaler[T]) UnmarshalBinary(data []byte) error {
	return nil
}

func main() {
	m := ValueMarshaler[[]byte]{value: []byte("string")}
	byteData, err := m.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("%b\n", byteData)
}
