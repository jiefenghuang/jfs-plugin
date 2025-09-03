//go:build !tmp

package server

import (
	"github.com/jiefenghuang/jfs-plugin/pkg/msg"
)

func newPlugin(pool msg.BytesPool) plugin {
	// TODO: implement plugin
	return nil
}
