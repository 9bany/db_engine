package types

import "fmt"

type UnsupportedDataTypeError struct {
	DataType string
}

func (u *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("unsupported data type %s", u.DataType)
}
