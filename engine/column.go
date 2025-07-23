package engine

import (
	"bytes"
	"fmt"

	"github.com/9bany/db/types"
)

const (
	ColumnNameLength byte = 64
)

type ColumnOptions struct {
	Nullable bool
}

type Column struct {
	name     [ColumnNameLength]byte
	dataType byte
	opts     ColumnOptions
}

type ColumnDefinitionMarshaler struct {
	Name      [64]byte
	DataType  byte
	AllowNull bool
}

func (c *ColumnDefinitionMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	typeFlag := NewValueMarshaler(types.TypeColumnDefinition)
	b, err := typeFlag.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	// len
	length := NewValueMarshaler(c.Size())
	b, err = length.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	colName := NewTLVMarshaler(string(c.Name[:]))
	b, err = colName.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	dataType := NewTLVMarshaler(c.DataType)
	b, err = dataType.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	allowNull := NewTLVMarshaler(c.AllowNull)
	b, err = allowNull.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	return buf.Bytes(), nil
}

func (c *ColumnDefinitionMarshaler) UnmarshalBinary(data []byte) error {
	var n uint32 = 0
	byteUnmarshaler := NewValueUnmarshaler[byte]()
	intUnmarshaler := NewValueUnmarshaler[uint32]()
	strUnmarshaler := NewValueUnmarshaler[string]()
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data[n : n+types.LenByte]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	dataType := byteUnmarshaler.Value
	n += types.LenByte
	if dataType != types.TypeColumnDefinition {
		return fmt.Errorf(
			"ColumnDefinitionMarshaler.UnmarshalBinary: expected: %d received: %d",
			types.TypeColumnDefinition,
			dataType,
		)
	}
	// length of struct
	if err := intUnmarshaler.UnmarshalBinary(data[n : n+types.LenInt32]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	n += types.LenInt32
	nameTLV := NewTLVUnmarshaler[string](strUnmarshaler)
	err := nameTLV.UnmarshalBinary(data[n:])
	if err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	name := nameTLV.Value
	n += nameTLV.BytesRead
	// unmarshal type
	typeTLV := NewTLVUnmarshaler[byte](byteUnmarshaler)
	err = typeTLV.UnmarshalBinary(data[n:])
	if err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	dataTypeVal := typeTLV.Value
	n += typeTLV.BytesRead
	// unmarshal allow null
	allowNullTLV := NewTLVUnmarshaler[byte](byteUnmarshaler)
	err = allowNullTLV.UnmarshalBinary(data[n:])
	if err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	allowNull := allowNullTLV.Value
	n += allowNullTLV.BytesRead
	copy(c.Name[:], name)
	c.DataType = dataTypeVal
	c.AllowNull = allowNull != 0
	return nil	
}

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return types.LenByte + types.LenInt32 + uint32(len(c.Name))
}
