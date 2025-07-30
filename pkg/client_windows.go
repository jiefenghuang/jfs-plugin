//go:build windows
// +build windows

package pkg

import "github.com/pkg/errors"

func checkProto(proto string) error {
	if proto != "tcp" {
		return errors.Errorf("windows only supports tcp protocol: got %s", proto)
	}
	return nil
}
