package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/9bany/db/internal/platform/parser"
	"github.com/9bany/db/internal/platform/parser/encoding"
	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table/column"
	columnio "github.com/9bany/db/internal/table/column/io"
)

var FileExtension string = ".bin"

type Tables []Table

type Columns map[string]*column.Column

type DeletableRecord struct {
	offset int64
	l      uint32
}

func newDeletableRecord(offset int64,
	l uint32) *DeletableRecord {
	return &DeletableRecord{
		offset: offset,
		l:      l,
	}
}

type Table struct {
	Name             string
	file             *os.File
	columnNames      []string
	columns          Columns
	reader           *parserio.Reader
	columnsDefReader *columnio.ColumnDefinitionReader
	recordParser     *parser.RecordParser
}

func fileNameWithoutExt(f *os.File) string {
	return strings.TrimSuffix(filepath.Base(f.Name()), filepath.Ext(f.Name()))
}

func NewTable(f *os.File, r *parserio.Reader, columnDefReader *columnio.ColumnDefinitionReader) (*Table, error) {
	return &Table{
		Name:             fileNameWithoutExt(f),
		file:             f,
		reader:           r,
		columnsDefReader: columnDefReader,
		columns:          make(Columns),
		columnNames:      make([]string, 0),
	}, nil
}

func NewTableWithColumns(f *os.File, columns Columns, columnNames []string) (*Table, error) {
	if len(columns) == 0 {
		return nil, NewCannotCreateTableError(nil, "table must have at least one column")
	}

	for _, col := range columns {
		if len(col.Name) == 0 {
			return nil, NewCannotCreateTableError(nil, "column name cannot be empty")
		}
	}

	return &Table{
		Name:        f.Name(),
		file:        f,
		columnNames: columnNames,
		columns:     columns,
	}, nil
}

func (t *Table) String() string {
	return fmt.Sprintf("Table{Name: %s, Columns: %v}", t.Name, t.columnNames)
}

func (t *Table) validateColumns(record map[string]interface{}) error {
	for colName, col := range t.columns {
		if _, ok := record[colName]; !ok {
			return fmt.Errorf("Table.validateColumns: column %s is missing in the record", colName)
		}
		if err := col.ValidateValue(record[colName]); err != nil {
			return fmt.Errorf("Table.validateColumns: %w", err)
		}
	}
	return nil
}

func (t *Table) ColumnNames() []string {
	return t.columnNames
}

func (t *Table) SetRecordParser(recParser *parser.RecordParser) error {
	if recParser == nil {
		return fmt.Errorf("Table.SetRecordParser: recParser cannot be nil")
	}
	t.recordParser = recParser
	return nil
}

func (t *Table) WriteColumnDefinitions(w io.Writer) error {
	for _, col := range t.columnNames {
		b, err := t.columns[col].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
		fmt.Println(b)
		writer := columnio.NewColumnDefinitionWriter(w)
		if n, err := writer.Write(b); n < len(b) || err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
	}
	return nil
}

func (t *Table) ReadColumnDefinitions() error {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
	}
	for {
		buf := make([]byte, 1024)
		n, err := t.columnsDefReader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		col := column.Column{}
		if err = col.UnmarshalBinary(buf[:n]); err != nil {
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		colName := col.NameToStr()
		t.columns[colName] = &col
		t.columnNames = append(t.columnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record map[string]interface{}) (int, error) {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if err := t.validateColumns(record); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	var sizeOfRecord uint32 = 0
	for _, col := range t.columnNames {
		val, ok := record[col]
		if !ok {
			return 0, fmt.Errorf("Table.Insert: missing column: %s", col)
		}
		tlvMarshaler := encoding.NewTLVMarshaler(val)
		length, err := tlvMarshaler.TLVLength()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		sizeOfRecord += length
	}

	buf := bytes.Buffer{}

	byteMarshaler := encoding.NewValueMarshaler(types.TypeRecord)
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(typeBuf)

	intMarshaler := encoding.NewValueMarshaler(sizeOfRecord)
	lenBuf, err := intMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(lenBuf)

	for _, col := range t.columnNames {
		v := record[col]
		tlvMarshaler := encoding.NewTLVMarshaler(v)
		b, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		buf.Write(b)
	}

	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if n != buf.Len() {
		return 0, columnio.NewIncompleteWriteError(n, buf.Len())
	}

	return 1, nil
}

func (t *Table) Select(
	whereStmt map[string]interface{},
) ([]map[string]interface{}, error) {
	if err := t.ensureFilePointer(); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}
	if err := t.validateWhereStmt(whereStmt); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}
	results := make([]map[string]interface{}, 0)

	for {
		err := t.recordParser.Parse()
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			return nil, fmt.Errorf("Table.Select: %w", err)
		}
		rawRecord := t.recordParser.Value
		if err = t.ensureColumnLength(rawRecord.Values); err != nil {
			return nil, fmt.Errorf("Table.Select: %w", err)
		}
		if !t.evaluateWhereStmt(whereStmt, rawRecord.Values) {
			continue
		}

		results = append(results, rawRecord.Values)
	}
}

func (t *Table) Delete(whereStmt map[string]interface{}) (int, error) {
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}
	if err := t.validateWhereStmt(whereStmt); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}

	deletableRecords := make([]*DeletableRecord, 0)
	for {
		if err := t.recordParser.Parse(); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}

		rawRecord := t.recordParser.Value
		if err := t.ensureColumnLength(rawRecord.Values); err != nil {
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}

		if !t.evaluateWhereStmt(whereStmt, rawRecord.Values) {
			continue
		}

		pos, err := t.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}
		deletableRecords = append(deletableRecords, newDeletableRecord(
			pos-int64(rawRecord.FullSize),
			rawRecord.FullSize,
		))
	}
	return t.markRecordDeleted(deletableRecords)
}

func (t *Table) Update(
	whereStmt map[string]interface{},
	values map[string]interface{},
) (int, error) {
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}
	if err := t.validateWhereStmt(whereStmt); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	deletableRecords := make([]*DeletableRecord, 0)
	rawRecords := make([]*parser.RawRecord, 0)
	for {
		if err := t.recordParser.Parse(); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("Table.Update: %w", err)
		}

		rawRecord := t.recordParser.Value
		if err := t.ensureColumnLength(rawRecord.Values); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}

		if !t.evaluateWhereStmt(whereStmt, rawRecord.Values) {
			continue
		}

		rawRecords = append(rawRecords, rawRecord)

		pos, err := t.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
		deletableRecords = append(deletableRecords, newDeletableRecord(
			pos-int64(rawRecord.FullSize),
			rawRecord.FullSize,
		))
	}

	if _, err := t.markRecordDeleted(deletableRecords); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	for _, rawRecord := range rawRecords {
		updatedRecord := make(map[string]interface{})
		for col, v := range rawRecord.Values {
			if value, ok := values[col]; ok {
				updatedRecord[col] = value
			} else {
				updatedRecord[col] = v
			}
		}
		if _, err := t.Insert(updatedRecord); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
	}
	return len(rawRecords), nil
}

func (t *Table) ensureFilePointer() error {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	if err := t.seekUntil(types.TypeRecord); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	return nil
}

func (t *Table) seekUntil(targetType byte) error {
	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("Table.seekUntil: readByte: %w", err)
		}
		if dataType == targetType {
			if _, err := t.file.Seek(-1*types.LenByte, io.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: %w", err)
			}
			return nil
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
		}

		if _, err := t.file.Seek(int64(length), io.SeekCurrent); err != nil {
			return fmt.Errorf("Table.seekUntil: %w", err)
		}
	}
}

func (t *Table) validateWhereStmt(whereStmt map[string]interface{}) error {
	for k := range whereStmt {
		if !slices.Contains(t.columnNames, k) {
			return fmt.Errorf("unknwon column in where statement: %s", k)
		}
	}
	return nil
}

func (t *Table) evaluateWhereStmt(
	whereStmt map[string]interface{},
	record map[string]interface{},
) bool {
	for k, v := range whereStmt {
		if record[k] != v {
			return false
		}
	}
	return true
}

func (t *Table) ensureColumnLength(record map[string]interface{}) error {
	if len(record) != len(t.columns) {
		return column.NewMismatchingColumnsError(len(t.columns), len(record))
	}
	return nil
}

func (t *Table) markRecordDeleted(deleableRecords []*DeletableRecord) (int, error) {
	for _, rec := range deleableRecords {
		if _, err := t.file.Seek(rec.offset, io.SeekStart); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		if err := binary.Write(t.file, binary.LittleEndian, types.TypeDeletedRecord); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		length, err := t.reader.ReadUint32()
		if err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		zeroBytes := make([]byte, length)
		if err = binary.Write(t.file, binary.LittleEndian, zeroBytes); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
	}
	return len(deleableRecords), nil
}
