package aptdata

import "github.com/coreos/bbolt"
import _ "github.com/vmihailenco/msgpack"
import "github.com/hashicorp/go-getter"
import _ "fmt"
import "net/url"

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

func OpenDB(path string) (db *AptDB, err error) {
	var boltDB *bolt.DB
	boltDB, err = bolt.Open(path, 0644, nil)
	return &AptDB{boltDB: boltDB}, err
}

func DownloadData(dataDir string) error {
	var u url.URL
	u.Scheme = "http"
	u.Host = "ourairports.com"
	u.Path = "/data/airports.csv"
	return getter.Get(dataDir, u)
}
