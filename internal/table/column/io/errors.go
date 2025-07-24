package io

import "fmt"

func NewIncompleteWriteError(written, expected int) error {
	return &IncompleteWriteError{
		written:  written,
		expected: expected,
	}
}

type IncompleteWriteError struct {
	written  int
	expected int
}

func (e *IncompleteWriteError) Error() string {
	return fmt.Sprintf("incomplete write: written %d bytes, expected %d bytes", e.written, e.expected)
}
