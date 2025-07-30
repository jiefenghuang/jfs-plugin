package cmd

import (
	"strings"

	"github.com/juicedata/jfs-plugin/pkg"
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

func splitAddr(addr string) (string, string) {
	if strings.HasPrefix(addr, "unix://") {
		return "unix", addr[7:]
	} else if strings.HasPrefix(addr, "tcp://") {
		return "tcp", addr[6:]
	}
	return "", ""
}

func runServer(c *cli.Context) error {
	if c.NArg() != 1 {
		return errors.Errorf("expected exactly one argument for address, got %d", c.NArg())
	}

	proto, addr := splitAddr(c.Args().First())
	if proto == "" || addr == "" {
		return errors.Errorf("invalid address format %s, expected 'tcp://<addr>' or 'unix://<path>'", c.Args().First())
	}

	var minVer, maxVer *version.Semver
	if c.IsSet("min-version") {
		minVer = version.Parse(c.String("min-version"))
	}
	if c.IsSet("max-version") {
		maxVer = version.Parse(c.String("max-version"))
	}

	capList := pkg.DefaultCliCapList
	if c.IsSet("buff-list") {
		capList = c.IntSlice("buff-list")
	}

	svr := pkg.NewServer(&pkg.SvrOptions{
		Proto:      proto,
		Addr:       addr,
		BuffList:   capList,
		MinVersion: minVer,
		MaxVersion: maxVer,
	})
	return svr.Start(nil)
}
