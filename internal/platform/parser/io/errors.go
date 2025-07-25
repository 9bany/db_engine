package io

type IncompleteReadError struct {
	exceptedBytes int
	actualBytes   int
}

func (e *IncompleteReadError) Error() string {
	return "incomplete read: expected " + string(e.exceptedBytes) + " bytes, got " + string(e.actualBytes)
}
