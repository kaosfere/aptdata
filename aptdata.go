package aptdata

import "github.com/coreos/bbolt"
import _ "github.com/vmihailenco/msgpack"
import "fmt"
import "os"
import "net/http"
import "io"
import "errors"
import "time"

type AptDB struct {
	boltDB    *bolt.DB
	populated bool
}

func (a *AptDB) Close() error {
	return a.boltDB.Close()
}

type ErrUnpopulated struct {
	msg string
}

func (e ErrUnpopulated) Error() (msg string) {
	return e.msg
}

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

func downloadDataFile(dataDir string, filename string, url string, c chan error) {
	// fmt.Println("Downloading", filename)
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
		c <- errors.New(fmt.Sprintf("response code %d for %s", response.StatusCode, url))
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

func OpenDB(path string, create bool) (db *AptDB, err error) {
	var boltDB *bolt.DB
	//populated := false

	boltDB, err = bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}

	err = boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Meta"))
		if b == nil {
			return ErrUnpopulated{"meta bucket does not exist"}
		}
		//v := b.Get([]byte("IsPopulated"))
		//fmt.Println(v)
		return nil
	})

	switch err.(type) {
	case ErrUnpopulated:
		if create {
			boltDB, err = createDB(path)
			if err != nil {
				return nil, err
			}
		}
	}

	return &AptDB{boltDB: boltDB}, err
}

func createDB(path string) (db *bolt.DB, err error) {
	return nil, nil
}

func DownloadData(dataDir string) (err error) {
	files := [4]string{"airports.csv", "runways.csv", "countries.sv", "regions.csv"}
	channels := make([]chan error, 4)

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
				numDownloaded += 1
				// fmt.Println("DID ONE", files[i]) // logging?
			default:
				time.Sleep(100 * time.Millisecond) // prevent spin-polling
			}
		}
	}
	return nil
}
