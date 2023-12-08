# data-migration-tool

## description

The tool name is dataMigrate. It is used to migrate InfluxDB data to openGemini.
The dataMigrate directly reads data from the TSM file of InfluxDB and writes the data to openGemini.

## quick start

### requirements

Go version > 1.16

Setting Environment Variables

```bash
> export GOPATH=/path/to/dir
> export GO111MODULE=on
> export GONOSUMDB=*
> export GOSUMDB=off
```

### compile

```bash
> bash build.sh
```

## data migration

Before migrating, you need to create the corresponding database and RP in openGemini. (This behavior may be fixed in a future release.)

```bash
> dataMigrate run --from dir/to/influxdb/data --to ip:port --database dbname
```

**WARNING**: When using this tool, please do not migrate data without shutting down InfluxDB if possible; otherwise, some
unknown problems may occur. To ensure that data is as complete as possible after migration, keep the empty write load
running before shutting down InfluxDB and wait for data in the cache to complete disk dumping (10 minutes by default).


### example 1: Migrate all databases

example influxdb data dir: `/var/lib/influxdb/data`

```bash
> ls -l /var/lib/influxdb/data
total 0
drwx------  4 root  root   128B 12  6 14:58 _internal
drwx------  4 root  root   128B 12  6 14:59 db0
drwx------  4 root  root   128B 12  8 09:01 db1
```

We migrate `internal` db

```bash
> ./dataMigrate run --from /var/lib/influxdb/data --to ip:port --database _internal

2023/12/08 14:17:48 Data migrate tool starting
2023/12/08 14:17:48 Debug mode is enabled
2023/12/08 14:17:48 Searching for tsm files to migrate
2023/12/08 14:17:48 Writing out data from shard _internal/monitor/1, [2/4]...
2023/12/08 14:17:48 Writing out data from shard db0/autogen/2, [4/4]...
2023/12/08 14:17:48 Writing out data from shard _internal/monitor/3, [3/4]...
2023/12/08 14:17:48 Writing out data from shard db1/autogen/5, [1/4]...
2023/12/08 14:17:48 Dealing file: /Users/shilinlee/.influxdb/data/_internal/monitor/1/000000001-000000001.tsm
2023/12/08 14:17:48 Dealing file: /Users/shilinlee/.influxdb/data/_internal/monitor/3/000000001-000000001.tsm
2023/12/08 14:17:48 Dealing file: /Users/shilinlee/.influxdb/data/db1/autogen/5/000000001-000000001.tsm
2023/12/08 14:17:48 Dealing file: /Users/shilinlee/.influxdb/data/db0/autogen/2/000000001-000000001.tsm
2023/12/08 14:17:48 Shard db0/autogen/2 takes 1.703084ms to migrate, with 1 tags, 2 fields, 2 rows read
2023/12/08 14:17:48 Shard db1/autogen/5 takes 2.076959ms to migrate, with 5 tags, 1 fields, 3 rows read
2023/12/08 14:17:48 Shard _internal/monitor/1 takes 467.09275ms to migrate, with 49 tags, 115 fields, 34098 rows read
2023/12/08 14:17:48 Shard _internal/monitor/3 takes 475.290791ms to migrate, with 49 tags, 115 fields, 22443 rows read
2023/12/08 14:17:48 Total: takes 477.482791ms to migrate, with 54 tags, 118 fields, 56546 rows read.
```

### example 2: Migrate the specified database

```bash
> ./dataMigrate run --from /var/lib/influxdb/data --to ip:port --database db0

2023/12/08 14:31:47 Data migrate tool starting
2023/12/08 14:31:47 Debug mode is enabled
2023/12/08 14:31:47 Searching for tsm files to migrate
2023/12/08 14:31:47 Writing out data from shard db0/autogen/2, [1/1]...
2023/12/08 14:31:47 Dealing file: /Users/shilinlee/.influxdb/data/db0/autogen/2/000000001-000000001.tsm
2023/12/08 14:31:47 Shard db0/autogen/2 takes 45.883209ms to migrate, with 1 tags, 2 fields, 2 rows read
2023/12/08 14:31:47 Total: takes 48.502792ms to migrate, with 1 tags, 2 fields, 2 rows read.
```

### example 3: Migrate the specified database with auth and https

```bash
> ./dataMigrate run --from /var/lib/influxdb/data --to ip:port --database db0 \
    --ssl --unsafeSsl --username rwusr --password This@123

2023/12/08 14:31:47 Data migrate tool starting
2023/12/08 14:31:47 Debug mode is enabled
2023/12/08 14:31:47 Searching for tsm files to migrate
2023/12/08 14:31:47 Writing out data from shard db0/autogen/2, [1/1]...
2023/12/08 14:31:47 Dealing file: /Users/shilinlee/.influxdb/data/db0/autogen/2/000000001-000000001.tsm
2023/12/08 14:31:47 Shard db0/autogen/2 takes 45.883209ms to migrate, with 1 tags, 2 fields, 2 rows read
2023/12/08 14:31:47 Total: takes 48.502792ms to migrate, with 1 tags, 2 fields, 2 rows read.
```


## For more help

```bash
Reads TSM files into InfluxDB line protocol format and write into openGemini

Usage:
  run [flags]

Flags:
      --batch int          Optional: specify batch size for inserting lines (default 1000)
      --database string    Optional: the database to read
      --debug              Optional: whether to enable debug log or not
      --end string         Optional: the end time to read (RFC3339 format)
  -f, --from string        Influxdb Data storage path. See your influxdb config item: data.dir (default "/var/lib/influxdb/data")
  -h, --help               help for run
  -p, --password string    Optional: The password to connect to the openGemini cluster.
      --retention string   Optional: the retention policy to read (required -database)
      --ssl                Optional: Use https for requests.
      --start string       Optional: the start time to read (RFC3339 format)
  -t, --to string          Destination host to write data to (default "127.0.0.1:8086")
      --unsafeSsl          Optional: Set this when connecting to the cluster using https and not use SSL verification.
  -u, --username string    Optional: The username to connect to the openGemini cluster.
```

**Welcome to add more features.**
