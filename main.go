package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

type QuoteAnime struct {
	Id        string
	Anime     string
	Character string
	Quote     string
}

func getConnection() *sql.DB {
	db, err := sql.Open("mysql", "root:4kWJkVilqTYs1HjQ9DkC@tcp(containers-us-west-53.railway.app:5609)/railway")
	fatalErr(err)

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)

	return db
}

func fatalErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	records := make(chan QuoteAnime)

	r, err := os.Open("lessreal-data.csv")
	fatalErr(err)

	go func() {
		parser := csv.NewReader(r)
		parser.Comma = ';'

		defer close(records)

		for {
			r, err := parser.Read()
			if err == io.EOF {
				break
			}
			fatalErr(err)

			var record QuoteAnime
			record.Id = r[0]
			record.Anime = r[1]
			record.Character = r[2]
			record.Quote = r[3]

			records <- record
		}
	}()

	print_record(records)
}

func print_record(records chan QuoteAnime) {

	db := getConnection()
	defer db.Close()

	stmt, err := db.Prepare("INSERT INTO quotes_anime (anime, character_name, quote) VALUES (?,?,?)")
	fatalErr(err)

	for record := range records {
		if record.Id == "" {
			continue
		}

		_, err := stmt.Exec(record.Anime, record.Character, record.Quote)
		fatalErr(err)
		log.Println("Data dengan id :", record.Id, "berhasil disimpan")

	}

	log.Println("Semua Data Berhasil Disimpan")
}
