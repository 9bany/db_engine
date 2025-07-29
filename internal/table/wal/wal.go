package wal

import "os"

type WAL struct {
	f *os.File
}

func (w *WAL) AppendLog(ops string, table string, data []byte) error {
	return nil
}
