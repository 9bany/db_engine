package encoding

import (
	"fmt"

	"github.com/9bany/db/internal/platform/parser/encoding"
	"github.com/9bany/db/internal/platform/types"
)

func NewColumnDefinitionUnmarshaler(name [64]byte, dataType byte, allowNull bool) *ColumnDefinitionUnmarshaler {
	return &ColumnDefinitionUnmarshaler{
		Name:      name,
		DataType:  dataType,
		AllowNull: allowNull,
	}
}

type ColumnDefinitionUnmarshaler struct {
	Name      [64]byte
	DataType  byte
	AllowNull bool
}

func (c *ColumnDefinitionUnmarshaler) UnmarshalBinary(data []byte) error {
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
