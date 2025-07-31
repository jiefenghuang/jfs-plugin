package msg

import "strings"

func SplitAddr(addr string) (string, string) {
	if strings.HasPrefix(addr, "unix://") {
		return "unix", addr[7:]
	} else if strings.HasPrefix(addr, "tcp://") {
		return "tcp", addr[6:]
	}
	return "", ""
}
