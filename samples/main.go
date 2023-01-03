package main

import (
	"database/sql"
	"fmt"
	"github.com/tidwall/buntdb"
)

type Book struct {
	Name      string `faker:"word,unique"`
	Date      sql.NullTime
	Author    string `fake:"{randomstring:[aa,ab]}"`
	Publisher string `fake:"{randomstring:[pa,pb]}"`
	Price     int
}

func main() {
	db, _ := buntdb.Open(":memory:")
	//db.CreateIndex("last_name_age", "*", buntdb.IndexJSON("name.last"), buntdb.IndexJSON("age"))
	db.CreateIndex("last_name", "*", buntdb.IndexJSON("name.last"))
	db.CreateIndex("age", "*", buntdb.IndexJSON("age"))
	db.Update(func(tx *buntdb.Tx) error {
		tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
		tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
		tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
		tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
		tx.Set("5", `{"name":{"first":"Sam","last":"Anderson"},"age":51}`, nil)
		tx.Set("6", `{"name":{"first":"Melinda6","last":"Prichard"},"age":44}`, nil)
		tx.Set("7", `{"name":{"first":"Melinda7","last":"Prichard"},"age":44}`, nil)
		tx.Set("8", `{"name":{"first":"Melinda8","last":"Prichard"},"age":44}`, nil)
		tx.Set("9", `{"name":{"first":"Melinda9","last":"Prichard"},"age":45}`, nil)
		return nil
	})
	db.View(func(tx *buntdb.Tx) error {
		fmt.Println("Order by last name")
		tx.Ascend("last_name", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age")
		tx.Ascend("age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age range 30-50")
		tx.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age equal")
		tx.AscendEqual("age", `{"age":44}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by last name equal")
		tx.AscendEqual("last_name", `{"name":{"last":"Prichard"}}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})
}
