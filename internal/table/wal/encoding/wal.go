package encoding

import (
	"bytes"
	"fmt"

	"github.com/9bany/db/internal/platform/parser/encoding"
	"github.com/9bany/db/internal/platform/types"
)

type Ops string

const (
	InsertOps Ops = "insert"
)

type WalMarshaler struct {
	Id    string
	Ops   Ops
	Table string
	Data  []byte
}

func (w *WalMarshaler) Marshaler() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	typeMarshaler := encoding.NewValueMarshaler(types.TypeWAL)
	typeData, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WalMarshaler.Marshaler: %w", err)
	}
	buf.Write(typeData)
	// Length
	length, err := w.len()
	if err != nil {
		return nil, fmt.Errorf("WalMarshaler.Marshaler: %w", err)
	}
	lengthMarshaler := encoding.NewValueMarshaler(length)
	legnthData, err := lengthMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WalMarshaler.Marshaler: %w", err)
	}
	buf.Write(legnthData)

	// Id
	idMarshal := encoding.NewTLVMarshaler(w.Id)
	idData, err := idMarshal.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WalMarshaler.Marshaler: %w", err)
	}
	buf.Write(idData)

	opsMarshal := encoding.NewTLVMarshaler(w.Ops)
	opsData, err := opsMarshal.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WalMarshaler.Marshaler: %w", err)
	}
	buf.Write(opsData)

	tableMarshal := encoding.NewTLVMarshaler(w.Table)
	tableData, err := tableMarshal.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WalMarshaler.Marshaler: %w", err)
	}
	buf.Write(tableData)
	buf.Write(w.Data)

	return buf.Bytes(), nil
}

func (w *WalMarshaler) len() (uint32, error) {
	idMarshal := encoding.NewTLVMarshaler(w.Id)
	lenId, err := idMarshal.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WalMarshaler.len: %w", err)
	}

	opsMarshal := encoding.NewTLVMarshaler(w.Ops)
	lengthOps, err := opsMarshal.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WalMarshaler.len: %w", err)
	}

	tableMarshal := encoding.NewTLVMarshaler(w.Table)
	lengthTable, err := tableMarshal.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WalMarshaler.len: %w", err)
	}

	return types.LenByte +
		types.LenInt32 +
		lenId +
		lengthOps +
		lengthTable +
		uint32(len(w.Data)), nil
}
