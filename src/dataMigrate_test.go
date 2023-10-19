/*
Copyright (c) 2013-2018 InfluxData Inc.
this code is originally from https://github.com/influxdata/influxdb/blob/1.8/cmd/influx_inspect/export/export.go
2023.03.08 Changed
Remove function writeWALFile
Remove write data to local file
Add insert values into openGemini
copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
*/
package src

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"net/http"
	"net/http/httptest"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type corpus map[string][]tsm1.Value

var (
	basicCorpus = corpus{
		tsm1.SeriesFieldKey("floats,k=f", "f"): []tsm1.Value{
			tsm1.NewValue(1, float64(1.5)),
			tsm1.NewValue(2, float64(3)),
		},
		tsm1.SeriesFieldKey("ints,k=i", "i"): []tsm1.Value{
			tsm1.NewValue(10, int64(15)),
			tsm1.NewValue(20, int64(30)),
		},
		tsm1.SeriesFieldKey("bools,k=b", "b"): []tsm1.Value{
			tsm1.NewValue(100, true),
			tsm1.NewValue(200, false),
		},
		tsm1.SeriesFieldKey("strings,k=s", "s"): []tsm1.Value{
			tsm1.NewValue(1000, "1k"),
			tsm1.NewValue(2000, "2k"),
		},
		tsm1.SeriesFieldKey("uints,k=u", "u"): []tsm1.Value{
			tsm1.NewValue(3000, uint64(45)),
			tsm1.NewValue(4000, uint64(60)),
		},
	}

	/*
	   In string field values, must escape:
	       * double quotes
	       * backslash character

	   In string field values, must escape:
	       * double quotes
	       * backslash character

	   In measurements, must escape:
	       * commas
	       * spaces
	*/

	escapeStringCorpus = corpus{
		tsm1.SeriesFieldKey(`"measurement\ with\ quo⚡es\ and\ emo\=ji",tag\ key\ wi\=th\ sp🚀ces=tag\,value\,wi\=th"commas"`, `field_k\ey`): []tsm1.Value{
			tsm1.NewValue(1, `1. "quotes"`),
			tsm1.NewValue(2, `2. back\slash`),
			tsm1.NewValue(3, `3. bs\q"`),
		},
		tsm1.SeriesFieldKey(`"measurement\,with\,quo⚡es\,and\,emo\=ji",tag\,key\,with\,"c💥mms"=tag\ value\ w💝th\ space`, `field_k\ey`): []tsm1.Value{
			tsm1.NewValue(4, `string field value, only " need be esc🍭ped`),
			tsm1.NewValue(5, `string field value, only """" need be escaped`),
		},
	}
)

func TestMain(m *testing.M) {
	fmt.Println("begin")
	logger = NewLogger()
	defer logger.Close()
	m.Run()
	fmt.Println("end")
}

func Test_ReadTSMFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			//_, _ = w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cmd := newCommand()
	cmd.setOutput(server.URL)

	for _, c := range []corpus{
		basicCorpus,
		escapeStringCorpus,
	} {
		tsmFile := writeCorpusToTSMFile(c)
		defer os.Remove(tsmFile.Name())

		filelist := []string{tsmFile.Name()}

		mig := NewMigrator(cmd)
		if err := mig.migrateTsmFiles(filelist); err != nil {
			t.Fatal(err)
		}
	}

	// Missing .tsm file should not cause a failure.
	filelist := []string{"file-that-does-not-exist.tsm"}
	if err := NewMigrator(newCommand()).migrateTsmFiles(filelist); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyMigrate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//time.Sleep(100 * time.Millisecond)
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			//_, _ = w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cmd := newCommand()
	cmd.startTime = 0
	cmd.endTime = 0
	cmd.setOutput(server.URL)

	f := writeCorpusToTSMFile(makeFloatsCorpus(100, 250))
	defer os.Remove(f.Name())

	filelist := []string{f.Name()}
	if err := NewMigrator(cmd).migrateTsmFiles(filelist); err != nil {
		t.Fatal(err)
	}
}

func benchmarkReadTSM(c corpus, b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//time.Sleep(100 * time.Millisecond)
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			//_, _ = w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cmd := newCommand()
	cmd.setOutput(server.URL)
	// Garbage collection is relatively likely to happen during export, so track allocations.
	b.ReportAllocs()

	f := writeCorpusToTSMFile(c)
	defer os.Remove(f.Name())

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		filelist := []string{f.Name()}
		if err := NewMigrator(cmd).migrateTsmFiles(filelist); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadTSMFloats_100s_250vps(b *testing.B) {
	benchmarkReadTSM(makeFloatsCorpus(100, 250), b)
}

func BenchmarkReadTSMInts_100s_250vps(b *testing.B) {
	benchmarkReadTSM(makeIntsCorpus(100, 250), b)
}

func BenchmarkReadTSMBools_100s_250vps(b *testing.B) {
	benchmarkReadTSM(makeBoolsCorpus(100, 250), b)
}

func BenchmarkReadTSMStrings_100s_250vps(b *testing.B) {
	benchmarkReadTSM(makeStringsCorpus(100, 250), b)
}

func newCommand() *DataMigrateCommand {
	return &DataMigrateCommand{
		Stderr:    ioutil.Discard,
		Stdout:    ioutil.Discard,
		manifest:  make([]fileGroupInfo, 0),
		tsmFiles:  make(map[string][]string),
		startTime: math.MinInt64,
		endTime:   math.MaxInt64,
		gstat:     &globalStatInfo{},
	}
}

// makeCorpus returns a new corpus filled with values generated by fn.
// The RNG passed to fn is seeded with numSeries * numValuesPerSeries, for predictable output.
func makeCorpus(numSeries, numValuesPerSeries int, fn func(*rand.Rand) interface{}) corpus {
	rng := rand.New(rand.NewSource(int64(numSeries) * int64(numValuesPerSeries)))
	var unixNano int64
	corpus := make(corpus, numSeries)
	for i := 0; i < numSeries; i++ {
		vals := make([]tsm1.Value, numValuesPerSeries)
		for j := 0; j < numValuesPerSeries; j++ {
			vals[j] = tsm1.NewValue(unixNano, fn(rng))
			unixNano++
		}

		k := fmt.Sprintf("m,t=%d", i)
		corpus[tsm1.SeriesFieldKey(k, "x")] = vals
	}

	return corpus
}

func makeFloatsCorpus(numSeries, numFloatsPerSeries int) corpus {
	return makeCorpus(numSeries, numFloatsPerSeries, func(rng *rand.Rand) interface{} {
		return rng.Float64()
	})
}

func makeIntsCorpus(numSeries, numIntsPerSeries int) corpus {
	return makeCorpus(numSeries, numIntsPerSeries, func(rng *rand.Rand) interface{} {
		// This will only return positive integers. That's probably okay.
		return rng.Int63()
	})
}

func makeBoolsCorpus(numSeries, numBoolsPerSeries int) corpus {
	return makeCorpus(numSeries, numBoolsPerSeries, func(rng *rand.Rand) interface{} {
		return rand.Int63n(2) == 1
	})
}

func makeStringsCorpus(numSeries, numStringsPerSeries int) corpus {
	return makeCorpus(numSeries, numStringsPerSeries, func(rng *rand.Rand) interface{} {
		// The string will randomly have 2-6 parts
		parts := make([]string, rand.Intn(4)+2)

		for i := range parts {
			// Each part is a random base36-encoded number
			parts[i] = strconv.FormatInt(rand.Int63(), 36)
		}

		// Join the individual parts with underscores.
		return strings.Join(parts, "_")
	})
}
func writeCorpusToTSMFile(c corpus) *os.File {
	tsmFile, err := ioutil.TempFile("", "export_test_corpus_tsm")
	if err != nil {
		panic(err)
	}

	w, err := tsm1.NewTSMWriter(tsmFile)
	if err != nil {
		panic(err)
	}

	// Write the series in alphabetical order so that each test run is comparable,
	// given an identical corpus.
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := w.Write([]byte(k), c[k]); err != nil {
			panic(err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		panic(err)
	}

	if err := w.Close(); err != nil {
		panic(err)
	}

	return tsmFile
}
