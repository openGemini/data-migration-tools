package src

import (
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/pkg/errors"
	"time"
)

type GeminiService interface {
	GetShardGroupDuration(database string) (time.Duration, error)
}

var _ GeminiService = (*geminiService)(nil)

type geminiService struct {
	out string
}

func NewGeminiService(cmd *DataMigrateCommand) *geminiService {
	return &geminiService{
		out: cmd.opt.Out,
	}
}

func (g *geminiService) GetShardGroupDuration(database string) (time.Duration, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://" + g.out,
	})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer c.Close()

	q := client.Query{
		Command:         "show retention policies",
		Database:        database,
		RetentionPolicy: "",
		Precision:       "",
		Chunked:         false,
		ChunkSize:       0,
		Parameters:      nil,
	}
	resp, err := c.Query(q)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	var shardGroupDuration time.Duration
	for _, item := range resp.Results {
		for _, item1 := range item.Series {
			for _, row := range item1.Values {
				if row[7] == "true" {
					shardGroupDuration, _ = time.ParseDuration(row[2].(string))
					break
				}
			}
		}
	}
	return shardGroupDuration, nil
}
