package encoding

import "fmt"

type UnsupportedDataTypeError struct {
	dataType string
}

func (u *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("unsupported data type %s", u.dataType)
}
