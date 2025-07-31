//go:build windows
// +build windows

package msg

import "github.com/pkg/errors"

func CheckProto(proto string) error {
	if proto != "tcp" {
		return errors.Errorf("windows only supports tcp protocol: got %s", proto)
	}
	return nil
}
