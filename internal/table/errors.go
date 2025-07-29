package table

import "fmt"

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

type InvalidFilename struct {
	filename string
}

func NewInvalidFilename(filename string) *InvalidFilename {
	return &InvalidFilename{filename: filename}
}

func (e *InvalidFilename) Error() string {
	return fmt.Sprintf("invalid filename: %s", e.filename)
}
