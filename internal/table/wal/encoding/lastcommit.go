package encoding

import (
	"bytes"
	"fmt"

	"github.com/9bany/db/internal/platform/parser/encoding"
	"github.com/9bany/db/internal/platform/types"
)

func NewLastCommitMashaler(id string,
	len uint32) *LastCommitMashaler {
	return &LastCommitMashaler{
		ID:  id,
		Len: len,
	}
}

type LastCommitMashaler struct {
	ID  string
	Len uint32
}

func (l *LastCommitMashaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	typeMarshaler := encoding.NewValueMarshaler(types.TypeWALLastIDItem)
	byteData, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMashaler.MarshalBinary: %w", err)
	}
	buf.Write(byteData)

	lenRecord, err := l.len()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMashaler.MarshalBinary: %w", err)
	}

	lenMarshaler := encoding.NewValueMarshaler(lenRecord)
	lenBuf, err := lenMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMashaler.MarshalBinary: %w", err)
	}

	buf.Write(lenBuf)

	idTVLMarshaler := encoding.NewTLVMarshaler(l.ID)
	idBuf, err := idTVLMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMashaler.MarshalBinary: %w", err)
	}

	buf.Write(idBuf)

	lenTVLMarshaler := encoding.NewTLVMarshaler(l.Len)
	lenBufData, err := lenTVLMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMashaler.MarshalBinary: %w", err)
	}
	buf.Write(lenBufData)

	return buf.Bytes(), nil
}

func (l *LastCommitMashaler) len() (uint32, error) {
	idTVLMarshaler := encoding.NewTLVMarshaler(l.ID)
	idLength, err := idTVLMarshaler.TLVLength()
	if err != nil {
		return 0, err
	}

	lenTVLMarshaler := encoding.NewTLVMarshaler(l.Len)
	lenLength, err := lenTVLMarshaler.TLVLength()
	if err != nil {
		return 0, err
	}
	return types.LenMeta + idLength + lenLength, nil
}
