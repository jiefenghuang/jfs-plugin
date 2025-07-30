//go:build !windows
// +build !windows

package pkg

import "github.com/pkg/errors"

func checkProto(proto string) error {
	if proto != "tcp" && proto != "unix" {
		return errors.Errorf("unsupported protocol: %s", proto)
	}
	return nil
}
