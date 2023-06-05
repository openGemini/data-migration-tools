/*
Copyright (c) 2013-2018 InfluxData Inc.
this code is originally from https://github.com/influxdata/influxdb/blob/1.8/cmd/influx_inspect/export/export.go
and https://github.com/influxdata/influxdb/tree/v1.8.2/models/point.go
and https://github.com/influxdata/influxdb/tree/v1.8.2/tsdb/engine/tsm1/file_store.go

2023.03.08 Changed
Remove function writeWALFile
Remove write data to local file
Add insert values into openGemini
copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
*/

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

const BATCHSIZE = 100

// escape set for tags
type escapeSet struct {
	k   [1]byte
	esc [2]byte
}

var (
	tagEscapeCodes = [...]escapeSet{
		{k: [1]byte{','}, esc: [2]byte{'\\', ','}},
		{k: [1]byte{' '}, esc: [2]byte{'\\', ' '}},
		{k: [1]byte{'='}, esc: [2]byte{'\\', '='}},
	}
)

type fileGroupInfo struct {
	db  string
	rp  string
	sid string
}

type shardGroupInfo struct {
	db       string
	rp       string
	sids     []string
	min, max time.Time
}

func (sgi *shardGroupInfo) Contains(t time.Time) bool {
	return !t.Before(sgi.min) && t.Before(sgi.max)
}

type globalStatInfo struct {
	progress   atomic.Int64
	tagsTotal  sync.Map
	fieldTotal sync.Map
	rowsTotal  atomic.Int64
}

type DataMigrateCommand struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dataDir         string
	out             string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64

	manifest []fileGroupInfo
	tsmFiles map[string][]string

	shardGroupDuration time.Duration
	shardGroups        []shardGroupInfo
	gstat              *globalStatInfo
}

// NewDataMigrateCommand returns a new instance of DataMigrateCommand.
func NewDataMigrateCommand() *DataMigrateCommand {
	return &DataMigrateCommand{
		Stderr: os.Stderr,
		Stdout: os.Stdout,

		manifest:    make([]fileGroupInfo, 0),
		tsmFiles:    make(map[string][]string),
		shardGroups: make([]shardGroupInfo, 0),
		gstat:       &globalStatInfo{},
	}
}

// Run executes the command.
func (cmd *DataMigrateCommand) Run(args ...string) error {
	var start, end string
	var debug string
	flag.StringVar(&cmd.dataDir, "from", "/var/lib/influxdb/data", "Data storage path")
	flag.StringVar(&cmd.out, "to", "127.0.0.1:8086", "Destination host to write data to")
	flag.StringVar(&cmd.database, "database", "", "Optional: the database to read")
	flag.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to read (requires -database)")
	flag.StringVar(&start, "start", "", "Optional: the start time to read (RFC3339 format)")
	flag.StringVar(&end, "end", "", "Optional: the end time to read (RFC3339 format)")
	flag.StringVar(&debug, "mode", "", "Optional: whether to enable debug log or not")

	flag.Usage = func() {
		fmt.Fprintf(cmd.Stdout, "Reads TSM files into InfluxDB line protocol format and insert into openGemini\n\n")
		fmt.Fprintf(cmd.Stdout, "Usage: %s [flags]\n\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	flag.Parse()

	// write params to log
	logger.LogString("Got param \"from\": "+cmd.dataDir, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"to\": "+cmd.out, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"database\": "+cmd.database, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"retention\": "+cmd.retentionPolicy, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"start\": "+start, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"end\": "+end, TOLOGFILE, LEVEL_INFO)

	// set defaults
	if start != "" {
		s, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return err
		}
		cmd.startTime = s.UnixNano()
	} else {
		cmd.startTime = math.MinInt64
	}
	if end != "" {
		e, err := time.Parse(time.RFC3339, end)
		if err != nil {
			return err
		}
		cmd.endTime = e.UnixNano()
	} else {
		// set end time to max if it is not set.
		cmd.endTime = math.MaxInt64
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	gs := NewGeminiService()
	shardGroupDuration, err := gs.GetShardGroupDuration(cmd.database, "autogen")
	if err != nil {
		return err
	}
	cmd.shardGroupDuration = shardGroupDuration

	if debug == "debug" || debug == "Debug" || debug == "DEBUG" {
		logger.SetDebug()
		logger.LogString("Debug mode is enabled", TOCONSOLE|TOLOGFILE, LEVEL_DEBUG)
	}

	// start the pprof tool
	go func() {
		err := http.ListenAndServe("localhost:6160", nil)
		if err != nil {
			logger.LogString("pprof started failed: "+err.Error(), TOCONSOLE|TOLOGFILE, LEVEL_ERROR)
		}
	}()

	return cmd.runMigrate()
}

func (cmd *DataMigrateCommand) setOutput(url string) {
	cmd.out = strings.TrimPrefix(url, "http://")
}

// Check whether the parameters are valid or not.
func (cmd *DataMigrateCommand) validate() error {
	if cmd.retentionPolicy != "" && cmd.database == "" {
		return fmt.Errorf("dataMigrate: must specify a db")
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return fmt.Errorf("dataMigrate: end time before start time")
	}
	return nil
}

func (cmd *DataMigrateCommand) runMigrate() error {
	st := time.Now()
	if err := cmd.walkTSMFiles(); err != nil {
		return err
	}
	if err := cmd.migrate(); err != nil {
		return err
	}
	eclipse := time.Since(st)

	tagsTotal := 0
	cmd.gstat.tagsTotal.Range(func(_, _ interface{}) bool {
		tagsTotal++
		return true
	})
	fieldTotal := 0
	cmd.gstat.fieldTotal.Range(func(_, _ interface{}) bool {
		fieldTotal++
		return true
	})
	logger.LogString("Total: takes "+eclipse.String()+" to migrate, with "+
		strconv.Itoa(tagsTotal)+" tags, "+strconv.Itoa(fieldTotal)+
		" fields, "+strconv.Itoa(int(cmd.gstat.rowsTotal.Load()))+" rows read.", TOCONSOLE|TOLOGFILE, LEVEL_INFO)
	return nil
}

func (cmd *DataMigrateCommand) walkTSMFiles() error {
	logger.LogString("Searching for tsm files to migrate", TOCONSOLE|TOLOGFILE, LEVEL_INFO)
	err := filepath.Walk(cmd.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(cmd.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 4 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}

		if dirs[0] == cmd.database || cmd.database == "" {
			if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1], dirs[2])
				cmd.tsmFiles[key] = append(cmd.tsmFiles[key], path)
				if len(cmd.tsmFiles[key]) == 1 {
					cmd.manifest = append(cmd.manifest, fileGroupInfo{
						db:  dirs[0],
						rp:  dirs[1],
						sid: dirs[2],
					})
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// sort by db first, then by rp, then by sid
	sort.Slice(cmd.manifest, func(i, j int) bool {
		dbCmp := strings.Compare(cmd.manifest[i].db, cmd.manifest[j].db)
		if dbCmp != 0 {
			return dbCmp < 0
		}
		rpCmp := strings.Compare(cmd.manifest[i].rp, cmd.manifest[j].rp)
		if rpCmp != 0 {
			return rpCmp < 0
		}
		sid_i, _ := strconv.Atoi(cmd.manifest[i].sid)
		sid_j, _ := strconv.Atoi(cmd.manifest[j].sid)
		return sid_i < sid_j
	})
	return nil
}

func (cmd *DataMigrateCommand) fileTimeRange(file string) (min, max int64, err error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		logger.LogString(fmt.Sprintf("unable to read %s, skipping: %s", file, err.Error()), TOLOGFILE|TOCONSOLE, LEVEL_ERROR)
		return 0, 0, errors.WithStack(err)
	}
	defer r.Close()

	min, max = r.TimeRange()
	return
}

func (cmd *DataMigrateCommand) shardTimeRange(files []string) (min, max int64, err error) {
	sort.Strings(files)
	if len(files) == 1 {
		return cmd.fileTimeRange(files[0])
	}
	min, _, err = cmd.fileTimeRange(files[0])
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	_, max, err = cmd.fileTimeRange(files[len(files)-1])
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return
}

func (cmd *DataMigrateCommand) shardGroupByTimestamp(timestamp time.Time) *shardGroupInfo {
	for i := range cmd.shardGroups {
		sgi := &cmd.shardGroups[i]
		if sgi.Contains(timestamp) {
			return &cmd.shardGroups[i]
		}
	}
	return nil
}

func (cmd *DataMigrateCommand) createShardGroupInfo(timestamp time.Time, info fileGroupInfo) shardGroupInfo {
	sgi := shardGroupInfo{
		db:   info.db,
		rp:   info.rp,
		sids: make([]string, 0),
	}
	sgi.min = timestamp.Truncate(cmd.shardGroupDuration).UTC()
	sgi.max = sgi.min.Add(cmd.shardGroupDuration).UTC()
	if sgi.max.After(time.Unix(0, models.MaxNanoTime)) {
		// Shard group range is [start, end) so add one to the max time.
		sgi.max = time.Unix(0, models.MaxNanoTime+1)
	}
	return sgi
}

func (cmd *DataMigrateCommand) populateShardGroups() error {
	for _, info := range cmd.manifest {
		key := filepath.Join(info.db, info.rp, info.sid)
		if files, ok := cmd.tsmFiles[key]; ok {
			min, _, err := cmd.shardTimeRange(files)
			if err != nil {
				return errors.WithStack(err)
			}
			minTs := time.Unix(0, min).UTC()
			sgi := cmd.shardGroupByTimestamp(minTs)
			if sgi != nil {
				sgi.sids = append(sgi.sids, info.sid)
				continue
			}
			newSgi := cmd.createShardGroupInfo(minTs, info)
			newSgi.sids = append(newSgi.sids, info.sid)
			cmd.shardGroups = append(cmd.shardGroups, newSgi)
		} else {
			logger.LogString("migrate: manifest does not match tsmFiles", TOLOGFILE, LEVEL_WARNING)
		}
	}
	return nil
}

func (cmd *DataMigrateCommand) doMigrate(ctx context.Context, info shardGroupInfo) error {
	migrateShard := func(key string, files []string) error {
		logger.LogString(fmt.Sprintf("Writing out data from shard %v, [%d/%d]...", key, cmd.gstat.progress.Inc(), len(cmd.manifest)), TOCONSOLE|TOLOGFILE, LEVEL_INFO)
		st := time.Now()

		mig := NewMigrator(cmd)
		defer mig.release()
		if err := mig.migrateTsmFiles(files); err != nil {
			return err
		}
		eclipse := time.Since(st)
		cmd.gstat.rowsTotal.Add(int64(mig.stat.rowsRead))

		logger.LogString("Shard "+key+" takes "+eclipse.String()+" to migrate, with "+
			strconv.Itoa(len(mig.stat.tagsRead))+" tags, "+strconv.Itoa(len(mig.stat.fieldsRead))+
			" fields, "+strconv.Itoa(mig.stat.rowsRead)+" rows read", TOCONSOLE|TOLOGFILE, LEVEL_INFO)
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		for _, sid := range info.sids {
			key := filepath.Join(info.db, info.rp, sid)
			if files, ok := cmd.tsmFiles[key]; ok {
				if err := migrateShard(key, files); err != nil {
					return errors.WithStack(err)
				}
			} else {
				logger.LogString("migrate: manifest does not match tsmFiles", TOLOGFILE|TOCONSOLE, LEVEL_WARNING)
			}
		}
		return nil
	}
}

func (cmd *DataMigrateCommand) migrate() error {
	if err := cmd.populateShardGroups(); err != nil {
		return errors.WithStack(err)
	}

	g, ctx := errgroup.WithContext(context.Background())
	sgiChan := make(chan shardGroupInfo)

	g.Go(func() error {
		defer close(sgiChan)
		for _, info := range cmd.shardGroups {
			sgiChan <- info
		}
		return nil
	})

	nWorkers := runtime.GOMAXPROCS(0)
	for i := 0; i < nWorkers; i++ {
		g.Go(func() error {
			for info := range sgiChan {
				if err := cmd.doMigrate(ctx, info); err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
	}

	return g.Wait()
}

func unescapeTag(in string) string {
	inb := []byte(in)
	if bytes.IndexByte(inb, '\\') == -1 {
		return in
	}

	for i := range tagEscapeCodes {
		c := &tagEscapeCodes[i]
		if bytes.IndexByte(inb, c.k[0]) != -1 {
			inb = bytes.Replace(inb, c.esc[:], c.k[:], -1)
		}
	}
	return string(inb)
}
