package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/9bany/db/internal/platform/parser"
	"github.com/9bany/db/internal/platform/parser/encoding"
	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table/column"
	columnio "github.com/9bany/db/internal/table/column/io"
	"github.com/9bany/db/internal/table/index"
	"github.com/9bany/db/internal/table/wal"
	walencoding "github.com/9bany/db/internal/table/wal/encoding"
)

const PageSize = 128

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
	Name        string
	file        *os.File
	columnNames []string
	columns     Columns

	reader           *parserio.Reader
	columnsDefReader *columnio.ColumnDefinitionReader
	recordParser     *parser.RecordParser
	wal              *wal.WAL
}

func NewTable(f *os.File,
	r *parserio.Reader,
	columnDefReader *columnio.ColumnDefinitionReader,
	wal *wal.WAL) (*Table, error) {

	tableName, err := GetTableName(f)
	if err != nil {
		return nil, fmt.Errorf("NewTable: %w", err)
	}

	return &Table{
		Name:             tableName,
		file:             f,
		reader:           r,
		columnsDefReader: columnDefReader,
		columns:          make(Columns),
		columnNames:      make([]string, 0),
		wal:              wal,
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

	entry, err := t.wal.AppendLog(walencoding.OpInsert, t.Name, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	_, err = t.insertIntoPage(buf)
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	if err := t.wal.Commit(entry); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
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

func GetTableName(f *os.File) (string, error) {
	// path/to/db/table.bin
	parts := strings.Split(f.Name(), ".")
	if len(parts) != 2 {
		return "", NewInvalidFilename(f.Name())
	}
	filenameParts := strings.Split(parts[0], "/")
	if len(filenameParts) == 0 {
		return "", NewInvalidFilename(f.Name())
	}
	return filenameParts[len(filenameParts)-1], nil
}

func (t *Table) RestoreWAL() error {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}

	restorableData, err := t.wal.GetRestorableData()
	if err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	// Nothing to restore
	if restorableData == nil {
		fmt.Printf("RestoreWAL skipped\n")
		return nil
	}

	n, err := t.file.Write(restorableData.Data)
	if err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	if n != len(restorableData.Data) {
		return fmt.Errorf("Table.RestoreWAL: %w", columnio.NewIncompleteWriteError(len(restorableData.Data), n))
	}

	fmt.Printf("RestoreWAL wrote %d bytes\n", n)

	if err = t.wal.Commit(restorableData.LastEntry); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}

	return nil
}
func (t *Table) updatePageSize(page int64, offset int32) (e error) {
	t.file.Seek(page, io.SeekStart)
	dataType, _ := t.reader.ReadByte()
	if dataType != types.TypePage {
		return fmt.Errorf("Table.updatePageSize: unexpected type: %d", dataType)
	}
	length, _ := t.reader.ReadUint32()
	_, err := t.file.Seek(-1*types.LenInt32, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	var newLength uint32
	if offset >= 0 {
		newLength = length + uint32(offset)
	} else {
		newLength = length - uint32(-offset)
	}
	marshaler := encoding.NewValueMarshaler[uint32](newLength)
	b, _ := marshaler.MarshalBinary()
	n, _ := t.file.Write(b)
	if n != len(b) {
		return columnio.NewIncompleteWriteError(len(b), n)
	}
	return nil

}

func (t *Table) insertIntoPage(buf bytes.Buffer) (*index.Page, error) {
	page, err := t.seekToNextPage(uint32(buf.Len()))
	if err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: %w", err)
	}
	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: file.Write: %w", err)
	}
	if n != buf.Len() {
		return nil, columnio.NewIncompleteWriteError(buf.Len(), n)
	}

	// seek back to the beginning of page
	if _, err = t.file.Seek(page.StartPos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: file.Seek: %w", err)
	}
	return page, t.updatePageSize(page.StartPos, int32(buf.Len()))
}

func (t *Table) seekToNextPage(lenToFit uint32) (*index.Page, error) {
	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("Table.seekToNextPage: %w", err)
	}

	for {
		err := t.seekUntil(types.TypeByte)
		if err != nil {
			if err == io.EOF {
				return t.insertEmptyPage()
			}
			return nil, fmt.Errorf("Table.seekToNextPage: %w", err)
		}

		// Skipping the type definition byte
		if _, err = t.reader.ReadByte(); err != nil {
			return nil, fmt.Errorf("Table.seekToNextPage: readByte: %w", err)
		}
		currPageLen, err := t.reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("Table.seekToNextPage: readUint32: %w", err)
		}

		if currPageLen+lenToFit <= PageSize {
			meta := int64(types.LenByte + types.LenInt32)
			pagePos, err := t.file.Seek(-1*meta, io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("Table.seekToNextPage: file.Seek: %w", err)
			}
			// This line is very important
			_, err = t.file.Seek(int64(currPageLen)+meta, io.SeekCurrent)
			return index.NewPage(pagePos), err
		}
	}
}

func (t *Table) insertEmptyPage() (*index.Page, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypePage); err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: type: %w", err)
	}

	// length
	if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: len: %w", err)
	}

	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: file.Write: %w", err)
	}
	if n != buf.Len() {
		return nil, columnio.NewIncompleteWriteError(buf.Len(), n)
	}

	currPos, err := t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: %w", err)
	}
	// startPos should point at the very first byte, that is types.TypePage and 5 bytes before the current pos
	startPos := currPos - (types.LenInt32 + types.LenByte)
	if startPos <= 0 {
		return nil, fmt.Errorf("Table.insertEmptyPage: unable to insert new page: start should be positive: %d", startPos)
	}
	return index.NewPage(startPos), nil
}
