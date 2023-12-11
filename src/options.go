package src

type DataMigrateOptions struct {
	DataDir         string
	Out             string
	Username        string
	Password        string
	Database        string
	RetentionPolicy string
	Start           string // rfc3339 format
	End             string // rfc3339 format
	StartTime       int64  // timestamp
	EndTime         int64  // timestamp
	BatchSize       int
	Ssl             bool
	UnsafeSsl       bool

	Debug bool
}
