/*
Package aptdata provides an abstracted interface to a database of airport
data.

Currently, support is provided for downloading data from ourairports.com, and
storing it as msgpacked data in a bolt database.  The actual storage backend is
only accessed through an interface we provide, so it should be transparent to
the user.
*/
package aptdata

import "github.com/coreos/bbolt"
import "github.com/vmihailenco/msgpack"
import "fmt"
import "os"
import "net/http"
import "io"
import "time"
import "encoding/csv"
import "strconv"
import "github.com/pkg/errors"

//AptDB provides an opaque wrapper around a boltdb database.  This is returned
//to the user from OpenDB().
type AptDB struct {
	boltDB *bolt.DB
}

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

//ErrUnpopulated provides a testable error condition for a database that exists
//but has not been fully populated with data.
type ErrUnpopulated struct {
	msg string
}

//Provides the string representation of the ErrUnpopulated error message.
func (e ErrUnpopulated) Error() (msg string) {
	return e.msg
}

//Close closes the connection to the airport database.
func (a *AptDB) Close() error {
	return a.boltDB.Close()
}

//Populated checks for the presence of an IsPopulated key in the Meta
//database, and if present confirms that it's true.
func (a *AptDB) Populated() bool {
	var isPopulated bool
	err := a.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Meta"))
		if b == nil {
			return ErrUnpopulated{"meta bucket does not exist"}
		}
		msgpack.Unmarshal(b.Get([]byte("IsPopulated")), &isPopulated)
		if isPopulated {
			return nil
		}
		return ErrUnpopulated{"populated flag false"}
	})

	if err != nil {
		return false
	}

	return true
}

//Load will process the downloaded airport and runways files and insert
//records for each entry in the database.
func (a *AptDB) Load(dataDir string) error {
	err := loadAirports(a.boltDB)
	if err != nil {
		return err
	}

	err = loadRunways(a.boltDB)
	if err != nil {
		return err
	}

	err = a.boltDB.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte("Meta"))
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte("Meta"))
		m, _ := msgpack.Marshal(true)
		err = b.Put([]byte("IsPopulated"), m)
		return err
	})

	return err
}

//Reload deletes existing entries in the database, then loads new records
//via a call to Load.
func (a *AptDB) Reload(dataDir string) error {
	err := a.boltDB.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte("Airports"))
		if err != nil {
			if err.Error() != "bucket not found" {
				return errors.Wrap(err, "airports bucket")
			}
		}
		err = tx.DeleteBucket([]byte("Runways"))
		if err != nil {
			if err.Error() != "bucket not found" {
				return errors.Wrap(err, "runways bucket")
			}
		}
		err = tx.DeleteBucket([]byte("Meta"))
		if err != nil {
			if err.Error() != "bucket not found" {
				return errors.Wrap(err, "meta bucket")
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	err = a.Load(dataDir)
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

//downloadDataFile is a utility function for downloading a source file and
//saving it to the specified data directory.
func downloadDataFile(dataDir string, filename string, url string, c chan error) {
	fullPath := fmt.Sprintf("%s/%s", dataDir, filename)
	out, err := os.Create(fullPath)
	if err != nil {
		c <- err
		return
	}
	defer out.Close()

	response, err := http.Get(url)
	if err != nil {
		c <- err
		return
	}
	if response.StatusCode != 200 {
		c <- fmt.Errorf("response code %d for %s", response.StatusCode, url)
		//c <- DownloadError{message: fmt.Sprintf("response code %d for %s", response.StatusCode, url)}
		out.Close()
		os.Remove(fullPath)
		return
	}
	defer response.Body.Close()

	_, err = io.Copy(out, response.Body)
	if err != nil {
		c <- err
		return
	}

	c <- nil
}

//OpenDB will open the boltdb and return it wrapped in an AptDB.
func OpenDB(path string) (db *AptDB, err error) {
	var boltDB *bolt.DB
	//populated := false

	boltDB, err = bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}

	return &AptDB{boltDB: boltDB}, err
}

//DownloadData iterates over the named source files and calls downloadDataFile
//for each one.
func DownloadData(dataDir string) (err error) {
	files := [4]string{"airports.csv", "runways.csv", "countries.csv", "regions.csv"}
	channels := make([]chan error, 4)

	_, err = os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	for i, file := range files {
		c := make(chan error)
		channels[i] = c
		go downloadDataFile(dataDir, file, fmt.Sprintf("http://ourairports.com/data/%s", file), c)
	}

	numDownloaded := 0
	for numDownloaded < len(files) {
		for _, c := range channels {
			select {
			case err = <-c:
				if err != nil {
					return err
				}
				numDownloaded++
				// fmt.Println("DID ONE", files[i]) // logging?
			default:
				time.Sleep(100 * time.Millisecond) // prevent spin-polling
			}
		}
	}
	return nil
}

//loadAirports processes airports.csv and creates an Airport struct
//representing each one which gets loaded into the Airports bucket in the
//database.
func loadAirports(db *bolt.DB) error {
	apts, err := os.Open("airports.csv")
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

//loadRunways processes runways.csv and creates a Runway struct
//representing each one which gets loaded into the Runways bucket in the
//database.
func loadRunways(db *bolt.DB) error {
	rwys, err := os.Open("runways.csv")
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
