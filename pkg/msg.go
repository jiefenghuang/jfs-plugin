package pkg

import (
	"unsafe"

	"github.com/juicedata/juicefs/pkg/utils"
)

// Msg: [Header] + [Body]
// Header: Cmd(1)+BodyLen(4)
// Body: [Param...]
// Param: Len + Value

const (
	cmdUnknown = iota
	cmdAuth    // for conn
	cmdInit
	cmdCreate
	cmdPut
	cmdGet
	cmdDel
	cmdCopy
	cmdStr
	cmdLimits
	cmdHead
	cmdSetSC
	cmdList
	cmdCreateMPU
	cmdUploadPart
	cmdUploadPartCopy
	cmdAbortMPU
	cmdCompleteMPU
	cmdListMPU
	cmdMax
)

var cmd2Name = map[byte]string{
	cmdUnknown:        "Unknown",
	cmdAuth:           "Auth",
	cmdInit:           "Init",
	cmdCreate:         "Create",
	cmdPut:            "Put",
	cmdGet:            "Get",
	cmdDel:            "Delete",
	cmdCopy:           "Copy",
	cmdStr:            "String",
	cmdLimits:         "Limits",
	cmdHead:           "Head",
	cmdSetSC:          "SetStorageClass",
	cmdList:           "List",
	cmdCreateMPU:      "CreateMultipartUpload",
	cmdUploadPart:     "UploadPart",
	cmdUploadPartCopy: "UploadPartCopy",
	cmdAbortMPU:       "AbortMultipartUpload",
	cmdCompleteMPU:    "CompleteMultipartUpload",
	cmdListMPU:        "ListMultipartUploads",
}

const (
	headerLen = 1 + 4
)

type msg struct {
	*utils.Buffer
}

func newMsg(data []byte) *msg {
	return &msg{
		Buffer: utils.FromBuffer(data),
	}
}

func newEncMsg(data []byte, bLen int, cmd byte) *msg {
	m := &msg{Buffer: utils.FromBuffer(data)}
	m.Put32(uint32(bLen))
	m.Put8(cmd)
	return m
}

func (m *msg) getHeader() (uint32, byte) {
	return m.Get32(), m.Get8()
}

func (m *msg) putBool(v bool) {
	if v {
		m.Put8(1)
	} else {
		m.Put8(0)
	}
}

func (m *msg) getBool() bool {
	return m.Get8() != 0
}

func (m *msg) putString(key string) {
	m.Put16(uint16(len(key)))
	if len(key) == 0 {
		return
	}
	m.Put(unsafe.Slice(unsafe.StringData(key), len(key)))
}

func (m *msg) getString() string {
	if !m.HasMore() {
		return ""
	}
	l := m.Get16()
	if l == 0 {
		return ""
	}
	return string(m.Get(int(l)))
}
