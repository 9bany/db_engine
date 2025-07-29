package types

import "fmt"

const (
	TypeInt64  byte = 1
	TypeString byte = 2
	TypeByte   byte = 3
	TypeBool   byte = 4
	TypeInt32  byte = 5
	TypeWAL    byte = 6

	TypeColumnDefinition byte = 99
	TypeRecord           byte = 100
	TypeDeletedRecord    byte = 101
)

const (
	LenByte  = 1
	LenInt32 = 4
	LenInt64 = 8
	LenMeta  = 5
)

func TypeBytes(value any) (byte, error) {
	switch v := any(value).(type) {
	case byte:
		return TypeByte, nil
	case int32:
		return TypeInt32, nil
	case int64:
		return TypeInt64, nil
	case string:
		return TypeString, nil
	case bool:
		return TypeBool, nil
	default:
		return 0, &UnsupportedDataTypeError{DataType: fmt.Sprint(v)}
	}
}

func TypeName(value any) string {
	switch any(value).(type) {
	case byte:
		return "TypeByte"
	case int32:
		return "TypeInt32"
	case int64:
		return "TypeInt64"
	case string:
		return "TypeString"
	case bool:
		return "TypeBool"
	default:
		return "Unsupported"
	}
}

func LengthData(value any) (uint32, error) {
	switch v := any(value).(type) {
	case byte:
		return 1, nil
	case int32:
		return 4, nil
	case int64:
		return 8, nil
	case string:
		return uint32(len(v)), nil
	case bool:
		return 1, nil
	default:
		return 0, &UnsupportedDataTypeError{DataType: fmt.Sprint(v)}
	}
}
