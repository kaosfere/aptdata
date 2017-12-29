package aptdata

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/coreos/bbolt"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

//Country maps country codes to names
type Country struct {
	Code string
	Name string
}

//loadCountries processes countries.csv and loads a Country struct into
//the Countries bucket in the DB for every record
func loadCountries(db *bolt.DB, dataDir string) error {
	countries, err := os.Open(fmt.Sprintf("%s/%s", dataDir, "countries.csv"))
	if err != nil {
		return err
	}
	defer countries.Close()

	r := csv.NewReader(countries)
	_, err = r.Read() // skip header

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte("Countries"))
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte("Countries"))

		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "country read")
			}

			country := Country{record[1], record[2]}

			m, err := msgpack.Marshal(&country)
			if err != nil {
				return errors.Wrap(err, "country marshal")
			}

			err = b.Put([]byte(record[1]), m)
			if err != nil {
				return errors.Wrap(err, "database put")
			}

		}

		return nil
	})

	return err
}

//GetCountry returns a Country struct representing the given code
func (a *AptDB) GetCountry(ident string) (*Country, error) {
	var country Country
	err := a.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Countries"))
		v := b.Get([]byte(ident))
		err := msgpack.Unmarshal(v, &country)
		return err
	})

	if err != nil {
		return &country, errors.Wrap(err, "get country")
	}

	return &country, nil
}
