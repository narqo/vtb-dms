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
	Phone      string    `json:"phone"`
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

	var p parser
	p.Parse(f)

	var (
		limiter = make(chan struct{}, 10)
		wg      sync.WaitGroup
	)

	for _, cc := range clinics {
		wg.Add(1)
		limiter <- struct{}{}
		go func(cc *Clinic) {
			if err := doGeocodeClinic(cc); err != nil {
				fmt.Fprintf(os.Stderr, "could not geocode clinic %q - %q: %v\n", cc.Name, cc.RawAddress, err)
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
	_MODE_NONE = iota
	_MODE_SECTION
	_MODE_NAME
	_MODE_ADDRESS
	_MODE_PHONE
)

type parser struct {
	nextMode int
}

func (p *parser) Parse(f io.Reader) {
	var cc Clinic
	r := bufio.NewScanner(f)
	for r.Scan() {
		line := r.Text()
		line = strings.TrimSpace(line)

		if line == "" {
			c := cc
			cc = Clinic{}
			clinics = append(clinics, &c)
			p.nextMode = _MODE_NAME
			continue
		} else if isSection(line) {
			// section
			p.nextMode = _MODE_NAME
			continue
		}

		switch p.nextMode {
		case _MODE_NAME:
			cc.Name = line
			p.nextMode = _MODE_ADDRESS
		case _MODE_ADDRESS:
			cc.RawAddress = line
			p.nextMode = _MODE_PHONE
		case _MODE_PHONE:
			cc.Phone = line
		}
	}
}

func isSection(line string) bool {
	if len(line) < 3 {
		return false
	}
	a, b, c := line[0], line[1], line[2]
	if unicode.IsNumber(rune(a)) {
		if b == '.' || (unicode.IsNumber(rune(b)) && c == '.') {
			return true
		}
	}
	return false
}

func doGeocodeClinic(cc *Clinic) error {
	if cc.RawAddress == "" {
		return fmt.Errorf("no raw address in clinic: %+v", cc)
	}
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

	if len(geoResp.Response.GeoObjectCollection.FeatureMember) == 0 {
		return fmt.Errorf("no geoobject in response: %+v", geoResp)
	}
	geoObj := geoResp.Response.GeoObjectCollection.FeatureMember[0].GeoObject

	rawPoints := strings.SplitN(geoObj.Point.Pos, " ", 2)
	if len(rawPoints) != 2 {
		return fmt.Errorf("bad points in response: %s", geoObj.Point.Pos)
	}
	var lat, long float64
	lat, err = strconv.ParseFloat(strings.TrimSpace(rawPoints[0]), 32)
	if err == nil {
		long, err = strconv.ParseFloat(strings.TrimSpace(rawPoints[1]), 32)
	}
	if err != nil {
		return err
	}

	cc.Points = []float64{long, lat}
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
