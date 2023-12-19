package cmd

import (
	"github.com/openGemini/dataMigrate/src"
	"github.com/spf13/cobra"
)

var (
	RootCmd *cobra.Command // represents the cluster command
	opt     src.DataMigrateOptions
)

func init() {
	RootCmd = &cobra.Command{
		Use:           "run",
		Short:         "Reads TSM files into InfluxDB line protocol format and write into openGemini",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			migrateCmd := src.NewDataMigrateCommand(&opt)
			if err := migrateCmd.Run(); err != nil {
				return err
			}
			return nil
		},
	}

	RootCmd.Flags().StringVarP(&opt.Username, "username", "u", "", "Optional: The username to connect to the openGemini cluster.")
	RootCmd.Flags().StringVarP(&opt.Password, "password", "p", "", "Optional: The password to connect to the openGemini cluster.")
	RootCmd.Flags().StringVarP(&opt.DataDir, "from", "f", "/var/lib/influxdb/data", "Influxdb Data storage path. See your influxdb config item: data.dir")
	RootCmd.Flags().StringVarP(&opt.Out, "to", "t", "127.0.0.1:8086", "Destination host to write data to")
	RootCmd.Flags().StringVarP(&opt.Database, "database", "", "", "the database to read")
	RootCmd.Flags().StringVarP(&opt.DestDatabase, "dest_database", "", "", "Optional: the database to write")
	RootCmd.Flags().StringVarP(&opt.RetentionPolicy, "retention", "", "", "Optional: the retention policy to read (required -database)")
	RootCmd.Flags().StringVarP(&opt.Start, "start", "", "", "Optional: the start time to read (RFC3339 format)")
	RootCmd.Flags().StringVarP(&opt.End, "end", "", "", "Optional: the end time to read (RFC3339 format)")
	RootCmd.Flags().IntVarP(&opt.BatchSize, "batch", "", 1000, "Optional: specify batch size for inserting lines")
	RootCmd.Flags().BoolVarP(&opt.Debug, "debug", "", false, "Optional: whether to enable debug log or not")
	RootCmd.Flags().BoolVarP(&opt.Ssl, "ssl", "", false, "Optional: Use https for requests.")
	RootCmd.Flags().BoolVarP(&opt.UnsafeSsl, "unsafeSsl", "", false, "Optional: Set this when connecting to the cluster using https and not use SSL verification.")
}

func Execute() error {
	return RootCmd.Execute()
}
