package main

import (
	"fmt"
	"github.com/golang/groupcache/lru"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	client "github.com/influxdata/influxdb1-client/v2"
	"os"
	"sort"
	"sync"
)

type Migrator interface {
	migrateTsmFiles(files []string) error
	writeCurrentFiles() error
	releaseTSMReaders()
	getDatabase() string
	getStat() *statInfo
	getGStat() *globalStatInfo
	getBatchSize() int
	release()
}

var _ Migrator = (*migrator)(nil)

var statPool = sync.Pool{
	New: func() interface{} {
		return &statInfo{
			tagsRead:   make(map[string]struct{}),
			fieldsRead: make(map[string]struct{}),
		}
	},
}

var filesPool = sync.Pool{
	New: func() interface{} {
		files := make([]tsm1.TSMFile, 0, 100)
		return &files
	},
}

var mstCachePool = sync.Pool{
	New: func() interface{} {
		return &lru.Cache{MaxEntries: 1000}
	},
}

var tagsCachePool = sync.Pool{
	New: func() interface{} {
		return &lru.Cache{MaxEntries: 1000}
	},
}

type statInfo struct {
	rowsRead   int
	tagsRead   map[string]struct{}
	fieldsRead map[string]struct{}
}

type migrator struct {
	out       string
	database  string
	startTime int64
	endTime   int64
	batchSize int

	files *[]tsm1.TSMFile
	// series to fields
	serieskeys map[string]map[string]struct{}
	// statistics
	stat  *statInfo
	gstat *globalStatInfo

	mstCache  *lru.Cache // measurement cache
	tagsCache *lru.Cache // tags cache
}

func (m *migrator) getBatchSize() int {
	return m.batchSize
}

func (m *migrator) release() {
	statPool.Put(m.stat)
	filesPool.Put(m.files)
	mstCachePool.Put(m.mstCache)
	tagsCachePool.Put(m.tagsCache)
}

func (m *migrator) getGStat() *globalStatInfo {
	return m.gstat
}

func (m *migrator) getDatabase() string {
	return m.database
}

func (m *migrator) getStat() *statInfo {
	return m.stat
}

func NewMigrator(cmd *DataMigrateCommand) *migrator {
	mig := &migrator{
		out:        cmd.out,
		database:   cmd.database,
		startTime:  cmd.startTime,
		endTime:    cmd.endTime,
		files:      filesPool.Get().(*[]tsm1.TSMFile),
		serieskeys: make(map[string]map[string]struct{}, 100),
		stat:       statPool.Get().(*statInfo),
		gstat:      cmd.gstat,
		batchSize:  cmd.batchSize,
		mstCache:   mstCachePool.Get().(*lru.Cache),
		tagsCache:  tagsCachePool.Get().(*lru.Cache),
	}
	mig.stat.rowsRead = 0
	mig.stat.tagsRead = make(map[string]struct{})
	mig.stat.fieldsRead = make(map[string]struct{})

	*mig.files = (*mig.files)[:0]
	return mig
}

func (m *migrator) migrateTsmFiles(files []string) error {
	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		// read all the TSMFiles using TSMReader
		logger.LogString(fmt.Sprintf("Dealing file: %s", f), TOCONSOLE|TOLOGFILE, LEVEL_INFO)
		if err := m.readTSMFile(f); err != nil {
			m.releaseTSMReaders()
			return err
		}
	}
	if err := m.writeCurrentFiles(); err != nil {
		return err
	}
	return nil
}

func (m *migrator) readTSMFile(tsmFilePath string) error {
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
	if sgStart, sgEnd := r.TimeRange(); sgStart > m.endTime || sgEnd < m.startTime {
		r.Close()
		return nil
	}

	*m.files = append(*m.files, r)

	// collect the keys
	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		series, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		seriesStr := string(series)
		if _, ok := m.serieskeys[seriesStr]; !ok {
			m.serieskeys[seriesStr] = make(map[string]struct{})
		}
		m.serieskeys[seriesStr][string(field)] = struct{}{}
	}
	return nil
}

func (m *migrator) releaseTSMReaders() {
	for _, r := range *m.files {
		r.Close()
	}
}

func (m *migrator) writeCurrentFiles() error {
	defer m.releaseTSMReaders()

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://" + m.out,
	})
	if err != nil {
		logger.LogString("Error creating openGemini Client: "+err.Error(), TOLOGFILE|TOCONSOLE, LEVEL_ERROR)
		return err
	}
	defer c.Close()

	for series, field := range m.serieskeys {
		var measurement interface{}
		var tags interface{}
		var ok bool
		if measurement, ok = m.mstCache.Get(series); !ok {
			measurement, tags, err = splitMeasurementAndTag(series)
			if err != nil {
				return err
			}
			m.mstCache.Add(series, measurement)
			m.tagsCache.Add(series, tags)
		}
		tags, _ = m.tagsCache.Get(series)

		// construct Scanner
		scanner := &Scanner{
			measurement: measurement.(string),
			tags:        tags.(map[string]string),
			fields:      make(map[string]*Cursor, len(field)),
			heapCursor: &heapCursor{
				items: make([]*Cursor, 0, len(field)),
			},
		}
		// construct field cursors
		for f := range field {
			key := tsm1.SeriesFieldKeyBytes(series, f)
			newCursor := &Cursor{
				et:     m.endTime,
				readTs: m.startTime,
				key:    key,
				seeks:  m.locations(key, m.startTime, m.endTime),
			}
			if err := newCursor.init(); err != nil {
				return err
			}
			scanner.fields[f] = newCursor
			scanner.heapCursor.items = append(scanner.heapCursor.items, newCursor)
		}
		if err := scanner.writeBatches(c, m); err != nil {
			return err
		}
	}
	return nil
}

// Referenced from the implementation of InfluxDB
func (m *migrator) locations(key []byte, st int64, et int64) []*location {
	var cache []tsm1.IndexEntry
	var locations []*location
	for _, fd := range *m.files {
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

// This function is used to split the underlying read SeriesKey with escape characters and convert it into a string that can be processed normally
func splitMeasurementAndTag(buf string) (measurement string, tags map[string]string, err error) {
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
