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

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return types.LenByte + types.LenInt32 + uint32(len(c.Name))
}
