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

package src

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

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

	opt *DataMigrateOptions

	manifest []fileGroupInfo
	tsmFiles map[string][]string

	shardGroupDuration time.Duration
	shardGroups        []shardGroupInfo
	gstat              *globalStatInfo
}

// NewDataMigrateCommand returns a new instance of DataMigrateCommand.
func NewDataMigrateCommand(opt *DataMigrateOptions) *DataMigrateCommand {
	return &DataMigrateCommand{
		Stderr: os.Stderr,
		Stdout: os.Stdout,

		opt: opt,

		manifest:    make([]fileGroupInfo, 0),
		tsmFiles:    make(map[string][]string),
		shardGroups: make([]shardGroupInfo, 0),
		gstat:       &globalStatInfo{},
	}
}

// Run executes the command.
func (cmd *DataMigrateCommand) Run() error {
	// set defaults
	if cmd.opt.Start != "" {
		s, err := time.Parse(time.RFC3339, cmd.opt.Start)
		if err != nil {
			return err
		}
		cmd.opt.StartTime = s.UnixNano()
	} else {
		cmd.opt.StartTime = math.MinInt64
	}
	if cmd.opt.End != "" {
		e, err := time.Parse(time.RFC3339, cmd.opt.End)
		if err != nil {
			return err
		}
		cmd.opt.EndTime = e.UnixNano()
	} else {
		// set end time to max if it is not set.
		cmd.opt.EndTime = math.MaxInt64
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	logger.LogString("Data migrate tool starting", TOCONSOLE, LEVEL_INFO)

	// write params to log
	logger.LogString("Got param \"from\": "+cmd.opt.DataDir, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"to\": "+cmd.opt.Out, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"database\": "+cmd.opt.Database, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"dest_database\": "+cmd.opt.DestDatabase, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"retention\": "+cmd.opt.RetentionPolicy, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"start\": "+cmd.opt.Start, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"end\": "+cmd.opt.End, TOLOGFILE, LEVEL_INFO)
	logger.LogString("Got param \"batch\": "+strconv.Itoa(cmd.opt.BatchSize), TOLOGFILE, LEVEL_INFO)

	gs := NewGeminiService(cmd)
	db := cmd.opt.Database
	if cmd.opt.DestDatabase != "" {
		db = cmd.opt.DestDatabase
	}
	shardGroupDuration, err := gs.GetShardGroupDuration(db)
	if err != nil {
		return err
	}
	cmd.shardGroupDuration = shardGroupDuration

	if cmd.opt.Debug {
		logger.SetDebug()
		logger.LogString("Debug mode is enabled", TOCONSOLE|TOLOGFILE, LEVEL_DEBUG)
	}

	// start the pprof tool
	go func() {
		err := http.ListenAndServe(":6160", nil)
		if err != nil {
			logger.LogString("pprof started failed: "+err.Error(), TOCONSOLE|TOLOGFILE, LEVEL_ERROR)
		}
	}()

	return cmd.runMigrate()
}

func (cmd *DataMigrateCommand) setOutput(url string) {
	cmd.opt.Out = strings.TrimPrefix(url, "http://")
}

// Check whether the parameters are valid or not.
func (cmd *DataMigrateCommand) validate() error {
	if cmd.opt.RetentionPolicy != "" && cmd.opt.Database == "" {
		return fmt.Errorf("dataMigrate: must specify a db")
	}
	if cmd.opt.DestDatabase == "" {
		cmd.opt.DestDatabase = cmd.opt.Database
	}
	if cmd.opt.StartTime != 0 && cmd.opt.EndTime != 0 && cmd.opt.EndTime < cmd.opt.StartTime {
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
	err := filepath.Walk(cmd.opt.DataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(cmd.opt.DataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 4 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}

		if (dirs[0] == cmd.opt.Database || cmd.opt.Database == "") &&
			(dirs[1] == cmd.opt.RetentionPolicy || cmd.opt.RetentionPolicy == "") {
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
	migrateShard := func(info *shardGroupInfo, key string, files []string) error {
		logger.LogString(fmt.Sprintf("Writing out data from shard %v, [%d/%d]...", key, cmd.gstat.progress.Inc(), len(cmd.manifest)), TOCONSOLE|TOLOGFILE, LEVEL_INFO)
		st := time.Now()

		mig := NewMigrator(cmd, info)
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
				if err := migrateShard(&info, key, files); err != nil {
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
