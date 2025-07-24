package column

import "github.com/9bany/db/internal/table/column/encoding"

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
		Name:     colName,
		dataType: dataType,
		opts:     opts,
	}
}

type Column struct {
	Name     [ColumnNameLength]byte
	dataType byte
	opts     ColumnOptions
}

func (c *Column) MarshalBinary() ([]byte, error) {
	marshaler := encoding.NewColumnDefinitionMarshaler(c.Name, c.dataType, c.opts.Nullable)
	return marshaler.MarshalBinary()
}
