/*
Package aptdata provides an abstracted interface to a database of airport
data.

Currently, support is provided for downloading data from ourairports.com, and
storing it as msgpacked data in a bolt database.  The actual storage backend is
only accessed through an interface we provide, so it should be transparent to
the user.
*/
package aptdata

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/coreos/bbolt"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

//AptDB provides an opaque wrapper around a boltdb database.  This is returned
//to the user from OpenDB().
type AptDB struct {
	boltDB *bolt.DB
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
	err := loadAirports(a.boltDB, dataDir)
	if err != nil {
		return err
	}

	err = loadRunways(a.boltDB, dataDir)
	if err != nil {
		return err
	}

	err = loadCountries(a.boltDB, dataDir)
	if err != nil {
		return err
	}

	err = loadRegions(a.boltDB, dataDir)
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
		err = tx.DeleteBucket([]byte("Countries"))
		if err != nil {
			if err.Error() != "bucket not found" {
				return errors.Wrap(err, "countries bucket")
			}
		}
		err = tx.DeleteBucket([]byte("Regions"))
		if err != nil {
			if err.Error() != "bucket not found" {
				return errors.Wrap(err, "regions bucket")
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
