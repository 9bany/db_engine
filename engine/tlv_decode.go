package engine

type TLVUnmarshaler[T any] struct {
	dataType    byte
	length      uint32
	Value       T
	unmarshaler *ValueUnmarshaler[T]
	BytesRead   uint32
}

func NewTLVUnmarshaler[T any](u *ValueUnmarshaler[T]) *TLVUnmarshaler[T] {
	return &TLVUnmarshaler[T]{
		unmarshaler: u,
	}
}

func (t *TLVUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	typeUnmarshal := &ValueUnmarshaler[byte]{}
	err := typeUnmarshal.UnmarshalBinary(data)
	if err != nil {
		return err
	}
	t.dataType = typeUnmarshal.Value

	t.BytesRead += 1

	lengthUnmarshal := &ValueUnmarshaler[uint32]{}
	err = lengthUnmarshal.UnmarshalBinary(data[t.BytesRead:])
	if err != nil {
		return err
	}
	t.length = lengthUnmarshal.Value
	t.BytesRead += 4

	err = t.unmarshaler.UnmarshalBinary(data[t.BytesRead:])
	if err != nil {

	}
	t.Value = t.unmarshaler.Value
	t.BytesRead += t.length
	return nil
}
