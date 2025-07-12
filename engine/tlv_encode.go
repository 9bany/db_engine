package engine

type TLVMarshaler[T any] struct {
	value          T
	valueMarshaler *ValueMarshaler[T]
}
