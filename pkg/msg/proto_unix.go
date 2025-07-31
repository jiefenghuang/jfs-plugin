//go:build !windows
// +build !windows

package msg

import "github.com/pkg/errors"

func CheckProto(proto string) error {
	if proto != "tcp" && proto != "unix" {
		return errors.Errorf("unsupported protocol: %s", proto)
	}
	return nil
}
