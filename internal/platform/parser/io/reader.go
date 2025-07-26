package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/9bany/db/internal/platform/types"
)

type Reader struct {
	reader io.Reader
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: reader}
}

func (r *Reader) Read(b []byte) (n int, err error) {
	if b == nil {
		return 0, fmt.Errorf("Reader.Read: nil buffer given")
	}
	n, err = r.reader.Read(b)
	if err != nil {
		return n, err
	}
	if n != len(b) {
		return n, fmt.Errorf(
			"Reader.Read: %w", &IncompleteReadError{exceptedBytes: len(b), actualBytes: n},
		)
	}
	return n, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	buf := make([]byte, types.LenInt32)
	if _, err := r.Read(buf); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (r *Reader) ReadByte() (byte, error) {
	buf := make([]byte, types.LenByte)
	if _, err := r.Read(buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *Reader) ReadTLV() ([]byte, error) {
	buf := bytes.Buffer{}

	byteData, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: %w", err)
	}
	buf.WriteByte(byteData)

	length, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: %w", err)
	}

	valBuf := make([]byte, length)
	if _, err := r.Read(valBuf); err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: %w", err)
	}
	buf.Write(valBuf)

	return buf.Bytes(), nil
}
