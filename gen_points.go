package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

var geocoderAPI *url.URL

func init() {
	geocoderAPI, _ = url.Parse("https://geocode-maps.yandex.ru/1.x/")
}

var (
	dataFile  = flag.String("in", "", "path to input file")
	outFile   = flag.String("out", "", "path to output file")
	outFormat = flag.String("format", "json", "output format")
	debug     = flag.Bool("debug", false, "debug")
)

type Clinic struct {
	Name       string    `json:"name"`
	RawAddress string    `json:"raw_address"`
	Address    string    `json:"address,omitempty"`
	Points     []float64 `json:"points"`
}

var clinics []*Clinic

func main() {
	flag.Parse()

	f, err := os.Open(*dataFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	p := newParser(f)
	if err := p.Parse(); err != nil {
		panic(err)
	}

	var (
		limiter = make(chan struct{}, 10)
		wg      sync.WaitGroup
	)

	for _, cc := range clinics {
		wg.Add(1)
		limiter <- struct{}{}
		go func(cc *Clinic) {
			if err := doGeocodeClinic(cc); err != nil {
				fmt.Fprintf(os.Stderr, "could not geocode clinic: %v", err)
			}
			<-limiter
			wg.Done()
		}(cc)
	}

	wg.Wait()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(clinics); err != nil {
		panic(err)
	}

	out := os.Stdout
	if *outFile != "" && *outFile != "-" {
		out, err = os.Create(*outFile)
		if err != nil {
			panic(err)
		}
		defer out.Close()
	}
	switch *outFormat {
	case "js":
		fmt.Fprintf(out, "data = %s", buf.String())
	case "json":
		io.Copy(out, &buf)
	default:
		fmt.Fprintf(os.Stderr, "unknown output format: %q", *outFormat)
	}
}

const (
	_MODE_NONE int = iota
	_MODE_SECTION
	_MODE_ADDRESS
)

type parser struct {
	s        *bufio.Scanner
	prevMode int
	mode     int
}

func newParser(f io.Reader) parser {
	return parser{
		s:        bufio.NewScanner(f),
		prevMode: _MODE_NONE,
		mode:     _MODE_NONE,
	}
}

func (p parser) Parse() error {
	for p.s.Scan() {
		line := p.s.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if unicode.IsNumber(rune(line[0])) && line[1] == '.' {
			p.prevMode = _MODE_NONE
			p.mode = _MODE_SECTION
		} else if p.mode == _MODE_SECTION {
			p.prevMode = p.mode
			p.mode = _MODE_ADDRESS
		}

		switch p.mode {
		case _MODE_SECTION:
		case _MODE_ADDRESS:
			var address string
			if p.s.Scan() {
				address = p.s.Text()
				address = strings.TrimSpace(address)
			}
			if address != "" {
				cc := &Clinic{
					Name:       line,
					RawAddress: address,
				}
				clinics = append(clinics, cc)
			}
		}
	}
	return p.s.Err()
}

func doGeocodeClinic(cc *Clinic) error {
	vals := make(url.Values)
	vals.Set("geocode", cc.RawAddress)
	vals.Set("lang", "ru_RU")
	vals.Set("kind", "house")
	vals.Set("format", "json")
	if *debug {
		println("geocoding", cc.RawAddress)
	}

	u := *geocoderAPI
	u.RawQuery = vals.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("bad response status: %s, %s", resp.Status, r)
	}

	var geoResp geocodeResponse
	err = json.NewDecoder(resp.Body).Decode(&geoResp)
	if err != nil {
		return err
	}

	geoObj := geoResp.Response.GeoObjectCollection.FeatureMember[0].GeoObject

	rawPoints := strings.SplitN(geoObj.Point.Pos, " ", 2)
	if len(rawPoints) != 2 {
		return fmt.Errorf("bad points in response: %s", geoObj.Point.Pos)
	}
	var p1, p2 float64
	p1, err = strconv.ParseFloat(strings.TrimSpace(rawPoints[0]), 32)
	if err == nil {
		p2, err = strconv.ParseFloat(strings.TrimSpace(rawPoints[1]), 32)
	}
	if err != nil {
		return err
	}

	cc.Points = []float64{p2, p1}
	cc.Address = geoObj.MetaDataProperty.GeocoderMetaData.Text

	return nil
}

type geocodeResponse struct {
	Response struct {
		GeoObjectCollection struct {
			FeatureMember []struct {
				GeoObject geoObject `json:"GeoObject"`
			} `json:"featureMember"`
		}
	} `json:"response"`
}

type geoObject struct {
	Name             string `json:"name"`
	Description      string `json:"description"`
	MetaDataProperty struct {
		GeocoderMetaData struct {
			Text string `json:"text"`
		} `json:"GeocoderMetaData"`
	} `json:"metaDataProperty"`
	Point struct {
		Pos string `json:"pos"`
	} `json:"Point"`
}
