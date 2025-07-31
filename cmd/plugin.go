package cmd

import (
	"github.com/jiefenghuang/jfs-plugin/pkg/server"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

func CmdPlugin() *cli.Command {
	return &cli.Command{
		Name:      "server",
		Action:    runServer,
		Category:  "TOOL",
		Usage:     "Start a object storage plugin server",
		ArgsUsage: "ADDR...",
		Flags: []cli.Flag{
			&cli.IntSliceFlag{
				Name:  "buff-list",
				Usage: "List of buffer sizes for encode/decode",
			},
			&cli.StringFlag{
				Name:  "min-version",
				Usage: "The minimum supported JuiceFS version",
				Value: "v1.3.0",
			},
			&cli.StringFlag{
				Name:  "max-version",
				Usage: "“The maximum supported JuiceFS version (not limited by default)",
				Value: "",
			},
		},
		Description: `
		./jfs-plugin server tcp://localhost:port
		./jfs-plugin server unix://path/to/socket`,
	}
}

func runServer(c *cli.Context) error {
	if c.NArg() != 1 {
		return errors.Errorf("expected exactly one argument for address, got %d", c.NArg())
	}

	var minVer, maxVer *version.Semver
	if c.IsSet("min-version") {
		minVer = version.Parse(c.String("min-version"))
	}
	if c.IsSet("max-version") {
		maxVer = version.Parse(c.String("max-version"))
	}

	var capList []int
	if c.IsSet("buff-list") {
		capList = c.IntSlice("buff-list")
	}

	svr, err := server.NewServer(&server.SvrOptions{
		URL:        c.Args().First(),
		BuffList:   capList,
		MinVersion: minVer,
		MaxVersion: maxVer,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create server")
	}
	return svr.Start(nil)
}
