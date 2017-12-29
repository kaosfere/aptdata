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

//Runway represents the fundamental data for a runway.
type Runway struct {
	Airport       string
	Length        int64
	Width         int64
	Surface       string
	Lighted       bool
	Closed        bool
	End1Name      string
	End1Latitude  float64
	End1Longitude float64
	End1Elevation int64
	End1Heading   int64
	End1Displaced int64
	End2Name      string
	End2Latitude  float64
	End2Longitude float64
	End2Elevation int64
	End2Heading   int64
	End2Displaced int64
}

//loadRunways processes runways.csv and creates a Runway struct
//representing each one which gets loaded into the Runways bucket in the
//database.
func loadRunways(db *bolt.DB, dataDir string) error {
	rwys, err := os.Open(fmt.Sprintf("%s/%s", dataDir, "runways.csv"))
	if err != nil {
		return err
	}
	defer rwys.Close()

	r := csv.NewReader(rwys)
	r.FieldsPerRecord = -1 // extra comma on first line
	_, err = r.Read()      // skip header

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("Runways"))
		if err != nil {
			fmt.Println(err)
			return err
		}
		//b := tx.Bucket([]byte("Runways"))

		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println(err)
				return errors.Wrap(err, "runway read")
			}

			length, _ := strconv.ParseInt(record[3], 10, 64)
			width, _ := strconv.ParseInt(record[4], 10, 64)
			lighted := record[6] == "1"
			closed := record[7] == "1"
			end1Latitude, _ := strconv.ParseFloat(record[9], 64)
			end1Longitude, _ := strconv.ParseFloat(record[10], 64)
			end1Elevation, _ := strconv.ParseInt(record[11], 10, 64)
			end1Heading, _ := strconv.ParseInt(record[12], 10, 64)
			end1Displaced, _ := strconv.ParseInt(record[13], 10, 64)
			end2Latitude, _ := strconv.ParseFloat(record[15], 64)
			end2Longitude, _ := strconv.ParseFloat(record[16], 64)
			end2Elevation, _ := strconv.ParseInt(record[17], 10, 64)
			end2Heading, _ := strconv.ParseInt(record[18], 10, 64)
			end2Displaced, _ := strconv.ParseInt(record[19], 10, 64)

			rwy := Runway{record[2],
				length,
				width,
				record[5],
				lighted,
				closed,
				record[8],
				end1Latitude,
				end1Longitude,
				end1Heading,
				end1Elevation,
				end1Displaced,
				record[14],
				end2Latitude,
				end2Longitude,
				end2Elevation,
				end2Heading,
				end2Displaced}

			m, err := msgpack.Marshal(&rwy)
			if err != nil {
				return errors.Wrap(err, "runway marshal")
			}

			b2, err := b.CreateBucketIfNotExists([]byte(record[2]))
			if err != nil {
				return errors.Wrap(err, "bucket creation")
			}
			err = b2.Put([]byte(record[8]+"/"+record[14]), m)
			if err != nil {
				return errors.Wrap(err, "database put")
			}

		}

		return nil
	})

	return err
}

//GetRunways returns a slice of Runway structs for a given airport.
func (a *AptDB) GetRunways(ident string) ([]*Runway, error) {
	var runways []*Runway
	err := a.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Runways"))
		b2 := b.Bucket([]byte(ident))
		b2.ForEach(func(k, v []byte) error {
			var rwy Runway
			msgpack.Unmarshal(v, &rwy)
			runways = append(runways, &rwy)
			return nil
		})
		return nil
	})

	if err != nil {
		return runways, errors.Wrap(err, "get runways")
	}

	return runways, nil
}
