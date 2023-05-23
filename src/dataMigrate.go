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
    "flag"
    "fmt"
    "io"
    "math"
    "os"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "time"

    "net/http"
    _ "net/http/pprof"

    "github.com/influxdata/influxdb/tsdb/engine/tsm1"
    client "github.com/influxdata/influxdb1-client/v2"
)

const BATCHSIZE = 5000

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

type statInfo struct {
    rowsRead   int
    tagsRead   map[string]struct{}
    fieldsRead map[string]struct{}
    rowsTotal  int
    tagsTotal  map[string]struct{}
    fieldTotal map[string]struct{}
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
    files    []tsm1.TSMFile
    // series to fields
    serieskeys map[string]map[string]struct{}
    // statistics
    stat statInfo
}

// NewDataMigrateCommand returns a new instance of DataMigrateCommand.
func NewDataMigrateCommand() *DataMigrateCommand {
    return &DataMigrateCommand{
        Stderr: os.Stderr,
        Stdout: os.Stdout,

        manifest:   make([]fileGroupInfo, 0),
        tsmFiles:   make(map[string][]string),
        files:      make([]tsm1.TSMFile, 0),
        serieskeys: make(map[string]map[string]struct{}),
        stat: statInfo{
            tagsRead:   make(map[string]struct{}),
            fieldsRead: make(map[string]struct{}),
            tagsTotal:  make(map[string]struct{}),
            fieldTotal: make(map[string]struct{}),
        },
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
    logger.LogString("Total: takes "+eclipse.String()+" to migrate, with "+
        strconv.Itoa(len(cmd.stat.tagsTotal))+" tags, "+strconv.Itoa(len(cmd.stat.fieldTotal))+
        " fields, "+strconv.Itoa(cmd.stat.rowsTotal)+" rows read.", TOCONSOLE|TOLOGFILE, LEVEL_INFO)
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

func (cmd *DataMigrateCommand) migrate() error {
    for i, info := range cmd.manifest {
        key := filepath.Join(info.db, info.rp, info.sid)
        if files, ok := cmd.tsmFiles[key]; ok {
            logger.LogString(fmt.Sprintf("Writing out data from shard %v, [%d/%d]...", key, i+1, len(cmd.manifest)), TOCONSOLE|TOLOGFILE, LEVEL_INFO)
            st := time.Now()
            if err := cmd.migrateTsmFiles(files); err != nil {
                return err
            }
            eclipse := time.Since(st)
            cmd.stat.rowsTotal += cmd.stat.rowsRead
            logger.LogString("Shard "+key+" takes "+eclipse.String()+" to migrate, with "+
                strconv.Itoa(len(cmd.stat.tagsRead))+" tags, "+strconv.Itoa(len(cmd.stat.fieldsRead))+
                " fields, "+strconv.Itoa(cmd.stat.rowsRead)+" rows read", TOCONSOLE|TOLOGFILE, LEVEL_INFO)
            cmd.stat.rowsRead = 0
            cmd.stat.tagsRead = make(map[string]struct{})
            cmd.stat.fieldsRead = make(map[string]struct{})
        } else {
            logger.LogString("migrate: manifest does not match tsmFiles", TOLOGFILE, LEVEL_WARNING)
        }
    }
    return nil
}

func (cmd *DataMigrateCommand) migrateTsmFiles(files []string) error {
    // we need to make sure we write the same order that the files were written
    sort.Strings(files)

    for _, f := range files {
        // read all of the TSMFiles using TSMReader
        logger.LogString(fmt.Sprintf("Dealing file: %s", f), TOCONSOLE|TOLOGFILE, LEVEL_INFO)
        if err := cmd.readTSMFile(f); err != nil {
            cmd.releaseTSMReaders()
            return err
        }
    }
    if err := cmd.writeCurrentFiles(); err != nil {
        return err
    }
    return nil
}

func (cmd *DataMigrateCommand) readTSMFile(tsmFilePath string) error {
    f, err := os.Open(tsmFilePath)
    if err != nil {
        if os.IsNotExist(err) {
            logger.LogString("readTSMFile: missing file skipped: "+tsmFilePath, TOLOGFILE, LEVEL_WARNING)
            return nil
        }
        return err
    }
    defer f.Close()

    r, err := tsm1.NewTSMReader(f)
    if err != nil {
        logger.LogString(fmt.Sprintf("unable to read %s, skipping: %s", tsmFilePath, err.Error()), TOLOGFILE|TOCONSOLE, LEVEL_ERROR)
        return nil
    }

    // If the time range of this file does not meet the conditions, abort reading.
    if sgStart, sgEnd := r.TimeRange(); sgStart > cmd.endTime || sgEnd < cmd.startTime {
        r.Close()
        return nil
    }

    cmd.files = append(cmd.files, r)

    // collect the keys
    for i := 0; i < r.KeyCount(); i++ {
        key, _ := r.KeyAt(i)
        series, field := tsm1.SeriesAndFieldFromCompositeKey(key)
        if _, ok := cmd.serieskeys[string(series)]; !ok {
            cmd.serieskeys[string(series)] = make(map[string]struct{})
        }
        cmd.serieskeys[string(series)][string(field)] = struct{}{}
    }
    return nil
}

func (cmd *DataMigrateCommand) writeCurrentFiles() error {
    defer cmd.releaseTSMReaders()

    c, err := client.NewHTTPClient(client.HTTPConfig{
        Addr: "http://" + cmd.out,
    })
    if err != nil {
        logger.LogString("Error creating openGemini Client: "+err.Error(), TOLOGFILE|TOCONSOLE, LEVEL_ERROR)
        return err
    }
    defer c.Close()

    for series, field := range cmd.serieskeys {
        measurement, tags, err := cmd.splitMeasurementAndTag(series)
        if err != nil {
            return err
        }

        // construct Scanner
        scanner := &Scanner{
            measurement: measurement,
            tags:        tags,
            fields:      make(map[string]*Cursor),
        }
        // construct field cursors
        for f := range field {
            key := tsm1.SeriesFieldKeyBytes(series, f)
            newCursor := &Cursor{
                et:     cmd.endTime,
                readTs: cmd.startTime,
                key:    key,
                seeks:  cmd.locations(key, cmd.startTime, cmd.endTime),
            }
            if err := newCursor.init(); err != nil {
                return err
            }
            scanner.fields[f] = newCursor
        }
        if err := scanner.writeBatches(c, cmd); err != nil {
            return err
        }
    }
    return nil
}

// Referenced from the implementation of InfluxDB
func (cmd *DataMigrateCommand) locations(key []byte, st int64, et int64) []*location {
    var cache []tsm1.IndexEntry
    var locations []*location
    for _, fd := range cmd.files {
        tombstones := fd.TombstoneRange(key)

        // This file could potential contain points we are looking for so find the blocks for
        // the given key.
        entries := fd.ReadEntries(key, &cache)
    LOOP:
        for i := 0; i < len(entries); i++ {
            ie := entries[i]

            // Skip any blocks only contain values that are tombstoned.
            for _, t := range tombstones {
                if t.Min <= ie.MinTime && t.Max >= ie.MaxTime {
                    continue LOOP
                }
            }

            // If the max time of a block is before where we are looking, skip
            // it since the data is out of our range
            if ie.MaxTime < st {
                continue
            }

            if ie.MinTime > et {
                continue
            }

            location := &location{
                r:     fd,
                entry: ie,
            }

            if st-1 < st {
                // mark everything before the seek time as read
                // so we can filter it out at query time
                location.readMax = st - 1
            } else {
                location.readMax = st
            }

            // Otherwise, add this file and block location
            locations = append(locations, location)
        }
    }
    return locations
}

func (cmd *DataMigrateCommand) releaseTSMReaders() {
    for _, r := range cmd.files {
        r.Close()
    }
    cmd.files = []tsm1.TSMFile{}
    cmd.serieskeys = map[string]map[string]struct{}{}
}

// This function is used to split the underlying read SeriesKey with escape characters and convert it into a string that can be processed normally
func (cmd *DataMigrateCommand) splitMeasurementAndTag(buf string) (measurement string, tags map[string]string, err error) {
    buf_runes := []rune(buf)
    splits := make([][]rune, 0)
    var buffer []rune
    escaping := false
    buf_runes = append(buf_runes, ',')
    tags = make(map[string]string)
    read := 0
    for i, r := range buf_runes {
        if !escaping && r == '\\' {
            escaping = true
            continue
        }
        if escaping {
            escaping = false
            continue
        }
        if r == ',' {
            buffer = make([]rune, i-read)
            copy(buffer, buf_runes[read:i])
            splits = append(splits, buffer)
            read = i + 1
        }
    }
    measurement = string(splits[0])
    if len(measurement) <= 0 {
        return "", nil, fmt.Errorf("splitMeasurementAndTag parse failed: measurement can not be nil")
    }
    for i, kv := range splits {
        if i == 0 {
            continue
        }
        escaping := false
        for i, r := range kv {
            if !escaping && r == '\\' {
                escaping = true
                continue
            }
            if escaping {
                escaping = false
                continue
            }
            if r == '=' {
                tagKey := string(kv[:i])
                tagValue := string(kv[i+1:])
                if len(tagKey) <= 0 || len(tagValue) <= 0 {
                    return "", nil, fmt.Errorf("splitMeasurementAndTag parse failed: empty tag key or tag value")
                }
                tags[unescapeTag(tagKey)] = unescapeTag(tagValue)
            }
        }
    }
    err = nil
    return
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
