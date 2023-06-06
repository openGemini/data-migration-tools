# data-migration-tool

## description

The tool name is dataMigrate. It is used to migrate InfluxDB data to openGemini.
The dataMigrate directly reads data from the TSM file of InfluxDB and writes the data to openGemini.

## quick start

### requirements

Go version >1.16

Setting Environment Variables

```
> export GOPATH=/path/to/dir
> export GO111MODULE=on
> export GONOSUMDB=*
> export GOSUMDB=off
```

### compile

```
> bash build.sh
```

### data migration

```
> dataMigrate --from path/to/tsm-file --to ip:port --database dbname
```

```
Usage: dataMigrate [flags]

  -database string
    	Optional: the database to read
  -end string
    	Optional: the end time to read (RFC3339 format)
  -from string
    	Data storage path (default "/var/lib/Influxdb/data")
  -retention string
    	Optional: the retention policy to read (requires -database)
  -start string
    	Optional: the start time to read (RFC3339 format)
  -batch int
    	Optional: specify batch size for inserting lines (default 1000)
  -mode string
      Optional: whether to enable debug log or not (set as "Debug" to enable it)
  -to string
    	Destination host to write data to (default "127.0.0.1:8086",which is the openGemini service default address)
```

**Notice**: When using this tool, please do not migrate data without shutting down InfluxDB if possible; otherwise, some
unknown problems may occur. To ensure that data is as complete as possible after migration, keep the empty write load
running before shutting down InfluxDB and wait for data in the cache to complete disk dumping (10 minutes by default).

**Welcome to add more features.**
