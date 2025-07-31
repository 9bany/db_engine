package encoding

import "testing"

func TestNewWALMarshaler(t *testing.T) {
	id := "test-id"
	op := OpInsert
	table := "test-table"
	data := []byte("test-data")

	marshaler := NewWALMarshaler(id, op, table, data)

	if marshaler.ID != id {
		t.Errorf("expected ID to be %s, got %s", id, marshaler.ID)
	}
	if marshaler.Op != op {
		t.Errorf("expected Op to be %s, got %s", op, marshaler.Op)
	}
	if marshaler.Table != table {
		t.Errorf("expected Table to be %s, got %s", table, marshaler.Table)
	}
	if string(marshaler.Data) != string(data) {
		t.Errorf("expected Data to be %s, got %s", string(data), string(marshaler.Data))
	}
}

func TestWALMarshaler_MarshalBinary(t *testing.T) {
	id := "test-id"
	op := OpInsert
	table := "test-table"
	data := []byte("test-data")

	marshaler := NewWALMarshaler(id, op, table, data)

	result, err := marshaler.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) == 0 {
		t.Errorf("expected non-empty result, got empty")
	}
}

func TestWALMarshaler_len(t *testing.T) {
	id := "test-id"
	op := OpInsert
	table := "test-table"
	data := []byte("test-data")

	marshaler := NewWALMarshaler(id, op, table, data)

	length, err := marshaler.len()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedLength := uint32(len(data)) // Adjust this based on the actual length calculation logic
	if length < expectedLength {
		t.Errorf("expected length to be at least %d, got %d", expectedLength, length)
	}
}
