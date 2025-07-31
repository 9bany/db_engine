package encoding

import (
	"bytes"
	"fmt"

	"github.com/9bany/db/internal/platform/parser/encoding"
	"github.com/9bany/db/internal/platform/types"
)

type LastCommitMarshaler struct {
	ID  string
	Len uint32
}
type LastCommitUnmarshaler struct {
	ID  string
	Len uint32
}

func NewLastCommitMarshaler(id string, len uint32) *LastCommitMarshaler {
	return &LastCommitMarshaler{
		ID:  id,
		Len: len,
	}
}

func (m *LastCommitMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeMarshaler := encoding.NewValueMarshaler(types.TypeWALLastIDItem)
	typeBuf, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMarshaler.MarshalBinary: typeMarshaler %w", err)
	}
	buf.Write(typeBuf)

	lenValue, err := m.len()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMarshaler.MarshalBinary: getLenValue %w", err)
	}

	lenMarshaler := encoding.NewValueMarshaler(lenValue)
	lenBuf, err := lenMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMarshaler.MarshalBinary: lenMarshaler %w", err)
	}
	buf.Write(lenBuf)

	idMarshaler := encoding.NewTLVMarshaler(m.ID)
	idBuf, err := idMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMarshaler.MarshalBinary: idMarshaler %w", err)
	}

	buf.Write(idBuf)

	recordLenMarshaler := encoding.NewTLVMarshaler(m.Len)
	recordLenBuf, err := recordLenMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("LastCommitMarshaler.MarshalBinary: recordLenBuf %w", err)
	}
	buf.Write(recordLenBuf)

	return buf.Bytes(), nil
}

func (l *LastCommitMarshaler) len() (uint32, error) {
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
	value := types.LenMeta + idLength + lenLength
	return value, nil
}

func NewLastCommitUnmarshaler() *LastCommitUnmarshaler {
	return &LastCommitUnmarshaler{}
}

func (u *LastCommitUnmarshaler) UnmarshalBinary(data []byte) error {
	var bytesRead uint32 = 0

	byteUnmarshaler := encoding.NewValueUnmarshaler[byte]()
	intUnmarshaler := encoding.NewValueUnmarshaler[uint32]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: type: %w", err)
	}
	bytesRead += types.LenByte

	// len
	if err := intUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: len: %w", err)
	}
	bytesRead += types.LenInt32

	// ID
	idUnmarshaler := encoding.NewTLVUnmarshaler(&encoding.ValueUnmarshaler[string]{})
	if err := idUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: ID: %w", err)
	}
	u.ID = idUnmarshaler.Value
	bytesRead += idUnmarshaler.BytesRead

	intUnmarshaler = encoding.NewValueUnmarshaler[uint32]()
	lenUnmarshaler := encoding.NewTLVUnmarshaler(intUnmarshaler)
	if err := lenUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: len of last record: %w", err)
	}
	u.Len = lenUnmarshaler.Value
	bytesRead += lenUnmarshaler.BytesRead

	return nil
}
