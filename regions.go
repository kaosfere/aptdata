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

//Region maps a region code to region name and local code
type Region struct {
	Code      string
	LocalCode string
	Name      string
	Country   string
}

//loadRegions processes regions.csv and loads a Region struct into
//the Regions bucket in the DB for every record
func loadRegions(db *bolt.DB, dataDir string) error {
	regions, err := os.Open(fmt.Sprintf("%s/%s", dataDir, "regions.csv"))
	if err != nil {
		return err
	}
	defer regions.Close()

	r := csv.NewReader(regions)
	_, err = r.Read() // skip header

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte("Regions"))
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte("Regions"))

		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "region read")
			}

			region := Region{record[1],
				record[2],
				record[3],
				record[5]}

			m, err := msgpack.Marshal(&region)
			if err != nil {
				return errors.Wrap(err, "region marshal")
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

//GetRegion returns a Region struct representing the given code
func (a *AptDB) GetRegion(ident string) (*Region, error) {
	var region Region
	err := a.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Regions"))
		v := b.Get([]byte(ident))
		err := msgpack.Unmarshal(v, &region)
		return err
	})

	if err != nil {
		return &region, errors.Wrap(err, "get region")
	}

	return &region, nil
}
