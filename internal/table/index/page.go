package index

type Page struct {
	StartPos int64
}

func NewPage(s int64) *Page {
	return &Page{StartPos: s}
}
