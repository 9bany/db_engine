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

func main() {
	// write the code to find row #2?
	content := "1;my_pod\n2;pod2\n3;another_pod\n"
	row := findRow(content, '3')
	log.Println(row)
}
