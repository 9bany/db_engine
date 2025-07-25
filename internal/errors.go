package internal

import "fmt"

func NewDatabaseAlreadyExistsError(name string) *DatabaseAlreadyExistsError {
	return &DatabaseAlreadyExistsError{name: name}
}

type DatabaseAlreadyExistsError struct {
	name string
}

func (e *DatabaseAlreadyExistsError) Error() string {
	return fmt.Sprintf("database %s already exists", e.name)
}

func NewTableAlreadyExistsError(name string) *TableAlreadyExistsError {
	return &TableAlreadyExistsError{name: name}
}

type TableAlreadyExistsError struct {
	name string
}

func (e *TableAlreadyExistsError) Error() string {
	return fmt.Sprintf("table %s already exists", e.name)
}

func NewCannotCreateTableError(err error, name string) *CannotCreateTableError {
	return &CannotCreateTableError{err: err, name: name}
}

type CannotCreateTableError struct {
	err  error
	name string
}

func (e *CannotCreateTableError) Error() string {
	return fmt.Sprintf("cannot create table %s: %v", e.name, e.err)
}

func NewCannotOpenTableError(err error, name string) *CannotOpenTableError {
	return &CannotOpenTableError{err: err, name: name}
}

type CannotOpenTableError struct {
	err  error
	name string
}

func (e *CannotOpenTableError) Error() string {
	return fmt.Sprintf("cannot open table %s: %v", e.name, e.err)
}

func NewCannotReadTableError(err error, name string) *CannotReadTableError {
	return &CannotReadTableError{err: err, name: name}
}

type CannotReadTableError struct {
	err  error
	name string
}

func (e *CannotReadTableError) Error() string {
	return fmt.Sprintf("cannot read table %s: %v", e.name, e.err)
}

func NewDatabaseDoesNotExistError(name string) *DatabaseDoesNotExistError {
	return &DatabaseDoesNotExistError{name: name}
}

type DatabaseDoesNotExistError struct {
	name string
}

func (e *DatabaseDoesNotExistError) Error() string {
	return fmt.Sprintf("database %s does not exist", e.name)
}
