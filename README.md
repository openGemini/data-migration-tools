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
    	Data storage path (default "/tmp/openGemini/data")
  -retention string
    	Optional: the retention policy to read (requires -database)
  -start string
    	Optional: the start time to read (RFC3339 format)
  -to string
    	Destination host to write data to (default "127.0.0.1:8086")
```
**Welcome to add more features.**
