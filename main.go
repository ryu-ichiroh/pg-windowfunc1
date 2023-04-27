package main

import (
	"github.com/go-pp/pp"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Book struct {
	BookID  uuid.UUID `gorm:"primarykey"`
	Title   string
	Authors []Author `gorm:"many2many:book_authors;foreignKey:book_id;joinForeignKey:book_id;References:author_id;joinReferences:author_id"`
}

type Author struct {
	AuthorID uuid.UUID `gorm:"primarykey"`
	Name     string
}

func main() {
	dsn := "postgresql://postgres:postgrespassword@localhost:5432/postgres"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	db = db.Debug()

	books := []*Book{}
	err = db.Preload("Authors").Find(&books, []uuid.UUID{uuid.MustParse("1FB112D1-54C9-4308-99C6-0163BFD0172D"), uuid.MustParse("554BC347-F36C-4766-B66F-D651C84C56BA")}).Error
	if err != nil {
		panic(err)
	}

	pp.Println(books)
}
