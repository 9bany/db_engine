package column

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
