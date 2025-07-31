package msg

import (
	"unsafe"

	"github.com/juicedata/juicefs/pkg/utils"
)

// Msg: [Header] + [Body]
// Header: Cmd(1)+BodyLen(4)
// Body: [Param...]
// Param: Len + Value

const (
	CmdUnknown = iota
	CmdAuth    // for conn
	CmdInit
	CmdCreate
	CmdPut
	CmdGet
	CmdDel
	CmdCopy
	CmdStr
	CmdLimits
	CmdHead
	CmdSetSC
	CmdList
	CmdCreateMPU
	CmdUploadPart
	CmdUploadPartCopy
	CmdAbortMPU
	CmdCompleteMPU
	CmdListMPU
	CmdMax
)

var Cmd2Name = map[byte]string{
	CmdUnknown:        "Unknown",
	CmdAuth:           "Auth",
	CmdInit:           "Init",
	CmdCreate:         "Create",
	CmdPut:            "Put",
	CmdGet:            "Get",
	CmdDel:            "Delete",
	CmdCopy:           "Copy",
	CmdStr:            "String",
	CmdLimits:         "Limits",
	CmdHead:           "Head",
	CmdSetSC:          "SetStorageClass",
	CmdList:           "List",
	CmdCreateMPU:      "CreateMultipartUpload",
	CmdUploadPart:     "UploadPart",
	CmdUploadPartCopy: "UploadPartCopy",
	CmdAbortMPU:       "AbortMultipartUpload",
	CmdCompleteMPU:    "CompleteMultipartUpload",
	CmdListMPU:        "ListMultipartUploads",
}

const (
	HeaderLen = 1 + 4
)

type Msg struct {
	*utils.Buffer
}

func NewMsg(data []byte) *Msg {
	return &Msg{
		Buffer: utils.FromBuffer(data),
	}
}

func NewEncMsg(data []byte, bLen int, cmd byte) *Msg {
	m := &Msg{Buffer: utils.FromBuffer(data)}
	m.Put32(uint32(bLen))
	m.Put8(cmd)
	return m
}

func (m *Msg) GetHeader() (uint32, byte) {
	return m.Get32(), m.Get8()
}

func (m *Msg) PutBool(v bool) {
	if v {
		m.Put8(1)
	} else {
		m.Put8(0)
	}
}

func (m *Msg) GetBool() bool {
	return m.Get8() != 0
}

func (m *Msg) PutString(key string) {
	m.Put16(uint16(len(key)))
	if len(key) == 0 {
		return
	}
	m.Put(unsafe.Slice(unsafe.StringData(key), len(key)))
}

func (m *Msg) GetString() string {
	if !m.HasMore() {
		return ""
	}
	l := m.Get16()
	if l == 0 {
		return ""
	}
	return string(m.Get(int(l)))
}

type Lener interface {
	Len() int
}
