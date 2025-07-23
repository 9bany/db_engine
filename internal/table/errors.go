package table

func NewCannotCreateTableError(err error, tableName string) error {
	return &CannotCreateTableError{
		tableName: tableName,
		err:       err,
	}
}

type CannotCreateTableError struct {
	tableName string
	err       error
}

func (e *CannotCreateTableError) Error() string {
	if e.err != nil {
		return "cannot create table " + e.tableName + ": " + e.err.Error()
	}
	return "cannot create table " + e.tableName
}
