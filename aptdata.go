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
	boltDB *bolt.DB
}

func (a *AptDB) Close() error {
	return a.boltDB.Close()
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
	fmt.Println("Downloading", filename)
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

func OpenDB(path string) (db *AptDB, err error) {
	var boltDB *bolt.DB
	boltDB, err = bolt.Open(path, 0644, nil)
	return &AptDB{boltDB: boltDB}, err
}

func DownloadData(dataDir string) (err error) {
	files := [4]string{"airports.csv", "runways.csv", "countries.csv", "regions.csv"}
	channels := make([]chan error, 4)

	for i, file := range files {
		c := make(chan error)
		channels[i] = c
		go downloadDataFile(dataDir, file, fmt.Sprintf("http://ourairports.com/data/%s", file), c)
	}

	numDownloaded := 0
	for numDownloaded < len(files) {
		for i, c := range channels {
			select {
			case err = <-c:
				if err != nil {
					return err
				}
				numDownloaded += 1
				//				fmt.Println("DID ONE", files[i]) // logging?
			default:
				time.Sleep(100 * time.Millisecond) // prevent spin-polling
			}
		}
	}
	return nil
}
