package encoding

import (
	"bytes"
	"encoding/binary"
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
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: type flag: %w", err)
	}
	buf.Write(b)
	// len
	length := encoding.NewValueMarshaler(c.Size())
	b, err = length.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: len: %w", err)
	}
	buf.Write(b)

	colName := encoding.NewTLVMarshaler(string(c.Name[:]))
	b, err = colName.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: column name: %w", err)
	}
	buf.Write(b)

	dataType := encoding.NewTLVMarshaler(c.DataType)
	b, err = dataType.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: data type: %w", err)
	}
	buf.Write(b)

	allowNull := encoding.NewTLVMarshaler(c.AllowNull)
	b, err = allowNull.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: allow null: %w", err)
	}
	buf.Write(b)

	return buf.Bytes(), nil
}

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return types.LenByte + // type of col name
		types.LenInt32 + // len of col name
		uint32(len(c.Name)) + // value of col name
		types.LenByte + // type of data type
		types.LenInt32 + // len of data type
		uint32(binary.Size(c.DataType)) + // value of data type
		types.LenByte + // type of allow null
		types.LenInt32 + // len of allow_null
		uint32(binary.Size(c.AllowNull)) // value of allow_null
}
