package column

import (
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table/column/encoding"
)

const (
	ColumnNameLength byte = 64
)

type ColumnOptions struct {
	Nullable bool
}

func NewColumn(name string, dataType byte, opts ColumnOptions) *Column {
	var colName [ColumnNameLength]byte
	copy(colName[:], name)
	return &Column{
		Name:        colName,
		dataType:    dataType,
		opts:        opts,
		marshaler:   encoding.NewColumnDefinitionMarshaler(colName, dataType, opts.Nullable),
		unmarshaler: encoding.NewColumnDefinitionUnmarshaler(colName, dataType, opts.Nullable),
	}
}

type Column struct {
	Name        [ColumnNameLength]byte
	dataType    byte
	opts        ColumnOptions
	marshaler   *encoding.ColumnDefinitionMarshaler
	unmarshaler *encoding.ColumnDefinitionUnmarshaler
}

func (c *Column) MarshalBinary() ([]byte, error) {
	return c.marshaler.MarshalBinary()
}

func (c *Column) UnmarshalBinary(buf []byte) error {
	return c.unmarshaler.UnmarshalBinary(buf)
}

func (c *Column) ValidateValue(value interface{}) error {
	if value == nil && c.opts.Nullable {
		return nil
	}
	typeByte, err := types.TypeBytes(value)
	if err != nil {
		return err
	}
	if typeByte != c.dataType {
		return &types.UnsupportedDataTypeError{DataType: string(typeByte)}
	}
	return nil
}
