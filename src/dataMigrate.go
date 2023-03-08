/*
Copyright (c) 2013-2018 InfluxData Inc.
this code is originally from https://github.com/influxdata/influxdb/blob/1.8/cmd/influx_inspect/export/export.go

2023.03.08 Changed
Remove function writeWALFile
Remove write data to local file
Add insert values into openGemini
copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
*/

package main

import (
    "flag"
    "fmt"
    "io"
    "math"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "time"

    "github.com/influxdata/influxdb/pkg/escape"
    "github.com/influxdata/influxdb/tsdb/engine/tsm1"
    client "github.com/influxdata/influxdb1-client/v2"
)

const BATCHSIZE = 200

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

    manifest map[string]struct{}
    tsmFiles map[string][]string
}

// NewDataMigrateCommand returns a new instance of DataMigrateCommand.
func NewDataMigrateCommand() *DataMigrateCommand {
    return &DataMigrateCommand{
        Stderr: os.Stderr,
	Stdout: os.Stdout,
	
	manifest: make(map[string]struct{}),
        tsmFiles: make(map[string][]string),
    }
}

// Run executes the command.
func (cmd *DataMigrateCommand) Run(args ...string) error {
    var start, end string
    flag.StringVar(&cmd.dataDir, "from", "/tmp/openGemini/data", "Data storage path")
    flag.StringVar(&cmd.out, "to", "127.0.0.1:8086", "Destination host to write data to")
    flag.StringVar(&cmd.database, "database", "", "Optional: the database to read")
    flag.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to read (requires -database)")
    flag.StringVar(&start, "start", "", "Optional: the start time to read (RFC3339 format)")
    flag.StringVar(&end, "end", "", "Optional: the end time to read (RFC3339 format)")

    flag.Usage = func() {
	fmt.Fprintf(cmd.Stdout, "Reads TSM files into InfluxDB line protocol format and insert into openGemini\n\n")
	fmt.Fprintf(cmd.Stdout, "Usage: %s [flags]\n\n", filepath.Base(os.Args[0]))
	flag.PrintDefaults()
    }

    flag.Parse()

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
    return cmd.read()
}

func (cmd *DataMigrateCommand) setOutput(url string){
    cmd.out = strings.TrimPrefix(url,"http://")
}

func (cmd *DataMigrateCommand) validate() error {
    if cmd.retentionPolicy != "" && cmd.database == "" {
	return fmt.Errorf("must specify a db")
    }
    if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
	return fmt.Errorf("end time before start time")
    }
    return nil
}

func (cmd *DataMigrateCommand) read() error {
    if err := cmd.walkTSMFiles(); err != nil {
	return err
    }
    return cmd.write()
}

func (cmd *DataMigrateCommand) walkTSMFiles() error {
    return filepath.Walk(cmd.dataDir, func(path string, f os.FileInfo, err error) error {
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
	if len(dirs) < 2 {
	    return fmt.Errorf("invalid directory structure for %s", path)
	}
	if dirs[0] == cmd.database || cmd.database == "" {
	    if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
		key := filepath.Join(dirs[0], dirs[1])
		cmd.manifest[key] = struct{}{}
		cmd.tsmFiles[key] = append(cmd.tsmFiles[key], path)
	    }
	}
	return nil
    })
}

func (cmd *DataMigrateCommand) write() error {

    for key := range cmd.manifest {
	if files, ok := cmd.tsmFiles[key]; ok {
	    fmt.Fprintf(cmd.Stdout, "writing out tsm file data for %s...\n", key)
	    if err := cmd.writeTsmFiles(files); err != nil {
		return err
	    }
	}
    }
    return nil
}

func (cmd *DataMigrateCommand) writeTsmFiles(files []string) error {
    fmt.Fprintln(cmd.Stdout, "begin insert tsm data into openGemini")

    // we need to make sure we write the same order that the files were written
    sort.Strings(files)
     
    for i, f := range files {
	fmt.Fprintf(cmd.Stdout,"Deal file: %s, [%d/%d]\n",f,i+1,len(files)) 
	if err := cmd.ReadTSMFile(f); err != nil {
	    return err
	}
    }
    return nil
}

func (cmd *DataMigrateCommand) writeValues(seriesKey []byte, field string, values []tsm1.Value) error {
    c, err := client.NewHTTPClient(client.HTTPConfig{
	Addr: "http://"+cmd.out,
    })
    if err != nil {
	fmt.Println("Error creating openGemini Client: ", err.Error())
    }
    defer c.Close()

    sk := string(seriesKey)
    //fmt.Println(sk,field)
    bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
	Database: cmd.database,
	Precision: "ns",
    })

    tags := map[string]string{}
    vs := strings.Split(sk,",")
    measurement := vs[0]
    for i,key_value := range vs {
	if i==0 {
	    continue
	}
	tag_key_values := strings.Split(key_value,"=")
	tags[tag_key_values[0]]=tag_key_values[1]
    }
    
    fields := map[string]interface{}{}
    count := 0;
    all := len(values)
    for i, value := range values {
	ts := value.UnixNano()
	if (ts < cmd.startTime) || (ts > cmd.endTime) {
	    continue
	}
	fields[field]=value.Value()
	pt, err := client.NewPoint(measurement, tags, fields, time.Unix(0,ts))
	if err != nil {
	    fmt.Println("Error: ", err.Error())
	}
	bp.AddPoint(pt)
	count = count + 1
	if( count == BATCHSIZE ){
	    err := c.Write(bp)
	    if  err != nil {
		fmt.Fprintf(cmd.Stdout,"insert error: %v",err)
		return err
	    }
	    count = 0
	}
	if i == all-1 {
	    err := c.Write(bp)
	    if err != nil {
		fmt.Fprintf(cmd.Stdout,"insert error: %v",err)
		return err
	    }
	}
    }

    return nil
}

func (cmd *DataMigrateCommand) ReadTSMFile(tsmFilePath string) error {
    f, err := os.Open(tsmFilePath)
    if err != nil {
        if os.IsNotExist(err) {
	    fmt.Fprintf(cmd.Stdout, "skipped missing file: %s", tsmFilePath)
	    return nil
	}
	return err
    }
    defer f.Close()

    r, err := tsm1.NewTSMReader(f)
    if err != nil {
        fmt.Fprintf(cmd.Stderr, "unable to read %s, skipping: %s\n", tsmFilePath, err.Error())
        return nil
    }
    defer r.Close()
    
    if sgStart, sgEnd := r.TimeRange(); sgStart > cmd.endTime || sgEnd < cmd.startTime {
	return nil
    }

    for i := 0; i < r.KeyCount(); i++ {
	key, _ := r.KeyAt(i)
	values, err := r.ReadAll(key)
	if err != nil {
	    fmt.Fprintf(cmd.Stderr, "unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
	    continue
	}
	measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	field = escape.Bytes(field)

	if err := cmd.writeValues( measurement, string(field), values); err != nil {
	    // An error from writeValues indicates an IO error, which should be returned.
	    return err
	}
    }
    return nil
}


