package column

const (
	ColumnNameLength byte = 64
)

type ColumnOptions struct {
	Nullable bool
}

type Column struct {
	Name     [ColumnNameLength]byte
	dataType byte
	opts     ColumnOptions
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
