package column

import "fmt"

type MismatchingColumnsError struct {
	expected int
	actual   int
}

func NewMismatchingColumnsError(expected, actual int) *MismatchingColumnsError {
	return &MismatchingColumnsError{expected: expected, actual: actual}
}

func (e *MismatchingColumnsError) Error() string {
	return fmt.Sprintf("column number mismatch: expected: %d, actual: %d", e.expected, e.actual)
}
