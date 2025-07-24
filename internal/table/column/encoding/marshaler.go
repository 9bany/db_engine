package encoding

import (
	"bytes"
	"fmt"

	"github.com/9bany/db/internal/platform/parser/encoding"
	"github.com/9bany/db/internal/platform/types"
)

func NewColumnDefinitionMarshaler(name [64]byte, dataType byte, allowNull bool) *ColumnDefinitionMarshaler {
	return &ColumnDefinitionMarshaler{
		Name:      name,
		DataType:  dataType,
		AllowNull: allowNull,
	}
}

type ColumnDefinitionMarshaler struct {
	Name      [64]byte
	DataType  byte
	AllowNull bool
}

func (c *ColumnDefinitionMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	typeFlag := encoding.NewValueMarshaler(types.TypeColumnDefinition)
	b, err := typeFlag.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	// len
	length := encoding.NewValueMarshaler(c.Size())
	b, err = length.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	colName := encoding.NewTLVMarshaler(string(c.Name[:]))
	b, err = colName.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	dataType := encoding.NewTLVMarshaler(c.DataType)
	b, err = dataType.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	allowNull := encoding.NewTLVMarshaler(c.AllowNull)
	b, err = allowNull.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)
	return buf.Bytes(), nil
}

func (c *ColumnDefinitionMarshaler) UnmarshalBinary(data []byte) error {
	var n uint32 = 0
	byteUnmarshaler := encoding.NewValueUnmarshaler[byte]()
	intUnmarshaler := encoding.NewValueUnmarshaler[uint32]()
	strUnmarshaler := encoding.NewValueUnmarshaler[string]()
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

	nameTLV := encoding.NewTLVUnmarshaler(strUnmarshaler)
	err := nameTLV.UnmarshalBinary(data[n:])
	if err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	name := nameTLV.Value
	n += nameTLV.BytesRead
	// unmarshal type
	typeTLV := encoding.NewTLVUnmarshaler(byteUnmarshaler)
	err = typeTLV.UnmarshalBinary(data[n:])
	if err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	dataTypeVal := typeTLV.Value
	n += typeTLV.BytesRead
	// unmarshal allow null
	allowNullTLV := encoding.NewTLVUnmarshaler(byteUnmarshaler)
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
