package aptdata

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/coreos/bbolt"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

//Airport represents the fundamental data for an airport.
type Airport struct {
	Code      string
	Name      string
	Latitude  float64
	Longitude float64
	Elevation int64
	City      string
	Region    string
	Country   string
	Continent string
	Iata      string
}

//loadAirports processes airports.csv and creates an Airport struct
//representing each one which gets loaded into the Airports bucket in the
//database.
func loadAirports(db *bolt.DB, dataDir string) error {
	apts, err := os.Open(fmt.Sprintf("%s/%s", dataDir, "airports.csv"))
	if err != nil {
		return err
	}
	defer apts.Close()

	r := csv.NewReader(apts)
	_, err = r.Read() // skip header

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte("Airports"))
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte("Airports"))

		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "airport read")
			}

			latitude, _ := strconv.ParseFloat(record[4], 64)
			longitude, _ := strconv.ParseFloat(record[5], 64)
			elevation, _ := strconv.ParseInt(record[6], 10, 64)
			apt := Airport{record[1],
				record[3],
				latitude,
				longitude,
				elevation,
				record[10],
				record[9],
				record[8],
				record[7],
				record[13]}

			m, err := msgpack.Marshal(&apt)
			if err != nil {
				return errors.Wrap(err, "airport marshal")
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

//GetAirport returns an Airport struct representing the given code
func (a *AptDB) GetAirport(ident string) (*Airport, error) {
	var apt Airport
	err := a.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Airports"))
		v := b.Get([]byte(ident))
		err := msgpack.Unmarshal(v, &apt)
		return err
	})

	if err != nil {
		return &apt, errors.Wrap(err, "get airport")
	}

	return &apt, nil
}
