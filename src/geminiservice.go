package src

import (
	"fmt"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/pkg/errors"
)

type GeminiService interface {
	GetShardGroupDuration(database string) (time.Duration, error)
}

var _ GeminiService = (*geminiService)(nil)

type geminiService struct {
	out      string
	username string
	password string
	useSsl   bool
}

func NewGeminiService(cmd *DataMigrateCommand) *geminiService {
	return &geminiService{
		out:      cmd.opt.Out,
		username: cmd.opt.Username,
		password: cmd.opt.Password,
		useSsl:   cmd.opt.Ssl,
	}
}

func (g *geminiService) getUrl() string {
	url := fmt.Sprintf("http://%s", g.out)
	if g.useSsl {
		url = fmt.Sprintf("https://%s", g.out)
	}
	return url
}

func (g *geminiService) GetShardGroupDuration(database string) (time.Duration, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:               g.getUrl(),
		InsecureSkipVerify: true,
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
