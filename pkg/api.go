package pkg

import (
	"io"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

type lener interface {
	Len() int
}

type lenReadCloser interface {
	lener
	io.ReadCloser
}

type putIn struct {
	key    string
	reader *io.LimitedReader // // need to consume all data in put
}

type putOut struct {
	rid string
	sc  string
}

type getIn struct {
	key    string
	offset int64
	limit  int64
}

type getOut struct {
	rc  lenReadCloser
	rid string
	sc  string
}

type initIn struct {
	endpoint  string
	accesskey string
	secretkey string
	token     string
}

type delOut struct {
	rid string
}

type copyIn struct {
	src string
	dst string
}

type obj struct {
	key   string
	size  int64
	mtime time.Time
	isDir bool
	sc    string
}

func (o *obj) Key() string          { return o.key }
func (o *obj) Size() int64          { return o.size }
func (o *obj) Mtime() time.Time     { return o.mtime }
func (o *obj) IsDir() bool          { return o.isDir }
func (o *obj) IsSymlink() bool      { return false }
func (o *obj) StorageClass() string { return o.sc }

type headOut struct {
	obj
}

type listIn struct {
	prefix     string
	startAfter string
	token      string
	delimiter  string
	limit      int64
	followLink bool
}

type listOut struct {
	isTruncated bool
	nextMarker  string
	objects     []obj
}

type uloadPartIn struct {
	key      string
	uploadID string
	num      int
	body     *io.LimitedReader // need to consume all data in uploadPart
}

type uploadPartCopyIn struct {
	dstKey   string
	uploadID string
	num      int
	srcKey   string
	off      int64
	size     int64
}

type abortMPUIn struct {
	key      string
	uploadID string
}

type completeMPUIn struct {
	key      string
	uploadID string
	parts    []*object.Part
}

type listMPUOut struct {
	nextMarker string
	parts      []*object.PendingPart
}

type plugin interface {
	init(any) (any, error)                    // in: *initIn, out: nil
	str(any) (any, error)                     // in: nil, out: string
	limits(any) (any, error)                  // in: nil, out: *object.Limits
	get(any) (any, error)                     // in: *getIn, out: *getOut/nil
	put(any) (any, error)                     // in: *putIn, out: *putOut/nil
	create(any) (any, error)                  // in: nil, out: nil
	delete(any) (any, error)                  // in: string, out: *delOut/nil
	copy(any) (any, error)                    // in: *copyIn, out: nil
	head(any) (any, error)                    // in: string, out: *headOut/nil
	setStorageClass(any) (any, error)         // in: string, out: nil
	list(any) (any, error)                    // in: *listIn, out: *listOut/nil
	createMultipartUpload(any) (any, error)   // in: string, out: *object.MultipartUpload/nil
	uploadPart(any) (any, error)              // in: *uloadPartIn, out: *object.Part/nil
	uploadPartCopy(any) (any, error)          // in: *uploadPartCopyIn, out: *object.Part/nil
	abortMultipartUpload(any) (any, error)    // in: *abortMPUIn, out: nil
	completeMultipartUpload(any) (any, error) // in: *completeMPUIn, out: nil
	listMultipartUploads(any) (any, error)    // in: string, out: *listMPUOut/nil
}
