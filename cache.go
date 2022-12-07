package daos

import "github.com/kelindar/column"

type Cache struct {
	*column.Collection
}

func (i *Cache) Save(t any) {
	panic("save a struct directly")
}

type Schema struct {
	Name    string
	Columns []map[string]*column.Column
}

func build(schema Schema) *Cache {

	panic("")
}
