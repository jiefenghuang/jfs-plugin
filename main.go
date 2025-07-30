package main

import (
	"io/fs"
	"os"

	"github.com/juicedata/jfs-plugin/cmd"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var Version = "0.1.0"
var logger = utils.GetLogger("jfs-plugin")

func main() {
	app := &cli.App{
		Name:                 "plugin",
		Usage:                "A JuiceFS object storage plugin",
		Version:              Version,
		Copyright:            "Apache License 2.0",
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log",
				Usage: "Log file path",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "enable debug log",
			},
		},
		Before: func(c *cli.Context) error {
			if c.Bool("debug") {
				utils.SetLogLevel(logrus.DebugLevel)
			}

			logFile := c.String("log")
			if logFile == "" {
				return nil
			}
			if !fs.ValidPath(logFile) {
				return errors.Errorf("invalid log file path: %s", logFile)
			}
			utils.SetOutFile(logFile)
			return nil
		},
		Commands: []*cli.Command{
			cmd.CmdPlugin(),
		},
	}
	if err := app.Run(os.Args); err != nil {
		logger.Fatal(err)
	}
}
