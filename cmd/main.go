package main

import (
	"fmt"

	"github.com/9bany/db/engine"
)

func main() {

	tlv := engine.NewTLVMarshaler("ohmy")
	data, err := tlv.MarshalBinary()
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}
