package main

import "log"

func findRow(content string, r byte) string {

	for i := 0; i < len(content); i++ {
		var row string
		var match bool
		if content[i] == '\n' {
			continue
		}

		if content[i] == r {
			match = true
		}

		for content[i] != '\n' {
			row += string(content[i])
			i++
		}
		if match {
			return row
		}
	}
	return ""
}

func findRowFixedSize(content string, r byte) string {
	log.Println("Find row in content fixed size format")
	for i := 0; i < len(content); i++ {
		if content[i] == '\n' {
			continue
		}
		if content[i] == r {
			s := content[i : i+17]
			return s
		}
	}

	return ""
}

func main() {
	// write the code to find row #2?
	content := "1;my_pod\n2;pod2\n3;another_pod\n"
	row := findRow(content, '3')
	log.Println(row)

	// fixed size
	content = "1my_pod..........2pod2............3another_pod....."
	row = findRowFixedSize(content, '3')
	log.Println(row)
}
