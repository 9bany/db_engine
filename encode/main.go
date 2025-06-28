package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type int32Marshaler struct {
	value int32
}

func (f *int32Marshaler) MarshalBinary() (data []byte, err error) {
	buffer := bytes.Buffer{}
	if err := binary.Write(&buffer, binary.LittleEndian, f.value); err != nil {
		return []byte{}, err
	}
	return buffer.Bytes(), nil
}

func (f *int32Marshaler) UnmarshalBinary(data []byte) error {
	return nil
}

func main() {
	m := int32Marshaler{value: 23}
	byteData, err := m.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("%b\n", byteData)
}
