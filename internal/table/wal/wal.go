package wal

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/9bany/db/internal/table/wal/encoding"
)

const (
	FilenameTmpl       = "%s_wal.bin"
	LastIDFilenameTmpl = "%s_wal_last_commit.bin"
)

type Entry struct {
	Id  string
	Len uint32
}

func newEntry(id string, d []byte) *Entry {
	return &Entry{
		Id:  id,
		Len: uint32(len(d)),
	}
}

func NewWal(dbPath, tableName string) (*WAL, error) {
	path := filepath.Join(dbPath, fmt.Sprintf(FilenameTmpl, tableName))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0777)
	if err != nil {
		f, err = os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("NewWal: %w", err)
		}
	}

	path = filepath.Join(dbPath, fmt.Sprintf(LastIDFilenameTmpl, tableName))
	lastCommitfile, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0777)
	if err != nil {
		f, err = os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("NewWal: %w", err)
		}
	}

	return &WAL{
		f:           f,
		lastCommitf: lastCommitfile,
	}, nil
}

type WAL struct {
	f           *os.File
	lastCommitf *os.File
}

func (w *WAL) AppendLog(ops encoding.Ops, table string, data []byte) (*Entry, error) {
	id, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("WAL.AppendLog: %w", err)
	}

	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("WAL.AppendLog: %w", err)
	}

	marshaler := encoding.NewWalMarshaler(id, ops, table, data)
	byteData, err := marshaler.Marshaler()
	if err != nil {
		return nil, fmt.Errorf("WAL.AppendLog: %w", err)
	}

	if err := w.write(byteData); err != nil {
		return nil, fmt.Errorf("WAL.AppendLog: %w", err)
	}

	return newEntry(id, data), nil
}

func (w *WAL) Commit(entry *Entry) error {
	marshaler := encoding.NewLastCommitMashaler(entry.Id, entry.Len)
	buf, err := marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("WAL.Commit: %w", err)
	}
	if err := os.WriteFile(w.lastCommitf.Name(), buf, 0644); err != nil {
		return fmt.Errorf("WAL.Commit: %w", err)
	}
	return nil
}

func (w *WAL) write(buf []byte) error {
	n, err := w.f.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL.write: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("WAL.write: incomplete write. expected: %d, actual: %d",
			n,
			len(buf))
	}
	return nil
}

func generateID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("wal.generateID: %w", err)
	}
	return hex.EncodeToString(b), nil
}
