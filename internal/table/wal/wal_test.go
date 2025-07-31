package wal

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/9bany/db/internal/platform/parser/encoding"
	"github.com/9bany/db/internal/platform/types"
	"github.com/stretchr/testify/assert"
)

func TestWAL(t *testing.T) {
	err := os.MkdirAll("./test", 0755)
	assert.Nil(t, err)
	f, err := os.Create("./test/tb_user_wal.bin")
	assert.Nil(t, err)
	assert.NotNil(t, f)
	defer func() {
		f.Close()
		os.Remove("./test/tb_user_wal.bin")
		os.RemoveAll("./test")
	}()

	wal, err := NewWal("./test", "tb_user")
	assert.Nil(t, err)
	entry, err := wal.AppendLog("insert", "tb_user", []byte{1, 2, 3, 4})
	assert.Nil(t, err)
	assert.NotNil(t, entry)
	assert.NotEmpty(t, entry.Id)
	assert.Equal(t, uint32(4), entry.Len)

	err = wal.Commit(entry)
	assert.Nil(t, err)

}

func dataByteRecord(record map[string]interface{}) ([]byte, error) {
	columnNames := []string{"id"}
	var sizeOfRecord uint32 = 0
	for _, col := range columnNames {
		val, ok := record[col]
		if !ok {
			return nil, fmt.Errorf("Table.Insert: missing column: %s", col)
		}
		tlvMarshaler := encoding.NewTLVMarshaler(val)
		length, err := tlvMarshaler.TLVLength()
		if err != nil {
			return nil, fmt.Errorf("Table.Insert: %w", err)
		}
		sizeOfRecord += length
	}

	buf := bytes.Buffer{}

	byteMarshaler := encoding.NewValueMarshaler(types.TypeRecord)
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(typeBuf)

	intMarshaler := encoding.NewValueMarshaler(sizeOfRecord)
	lenBuf, err := intMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(lenBuf)

	for _, col := range columnNames {
		v := record[col]
		tlvMarshaler := encoding.NewTLVMarshaler(v)
		b, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("Table.Insert: %w", err)
		}
		buf.Write(b)
	}

	return buf.Bytes(), nil
}

func TestWALRestore(t *testing.T) {
	err := os.MkdirAll("./test", 0755)
	assert.Nil(t, err)
	f, err := os.Create("./test/tb_user_wal.bin")
	assert.Nil(t, err)
	assert.NotNil(t, f)
	defer func() {
		f.Close()
		os.Remove("./test/tb_user_wal.bin")
		os.RemoveAll("./test")
	}()

	wal, err := NewWal("./test", "tb_user")
	assert.Nil(t, err)

	func() {
		data, err := dataByteRecord(map[string]interface{}{
			"id": int32(1),
		})
		assert.Nil(t, err)
		entry, err := wal.AppendLog("insert", "tb_user", data)
		assert.Nil(t, err)
		assert.NotNil(t, entry)
		assert.NotEmpty(t, entry.Id)
		assert.Equal(t, uint32(79), entry.Len)

		err = wal.Commit(entry)
		assert.Nil(t, err)

	}()

	// fake log and not commit
	data, err := dataByteRecord(map[string]interface{}{
		"id": int32(3),
	})
	assert.Nil(t, err)
	entry2, err := wal.AppendLog("insert", "tb_user", data)
	assert.Nil(t, err)
	assert.NotNil(t, entry2)
	assert.NotEmpty(t, entry2.Id)
	assert.Equal(t, uint32(79), entry2.Len)
	// let restore
	restorableData, err := wal.GetRestorableData()

	assert.Nil(t, err)
	assert.NotNil(t, restorableData)
	log.Println(restorableData)
	assert.Equal(t, []byte{100, 9, 0, 0, 0, 5, 4, 0, 0, 0, 3, 0, 0, 0}, restorableData.Data)
}
