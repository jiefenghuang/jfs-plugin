//go:build tmp

package server

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jiefenghuang/jfs-plugin/pkg/msg"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/pkg/errors"
)

func newPlugin(pool msg.BytesPool) plugin {
	return &fPlugin{
		pool:     pool,
		objects:  make(map[string][]byte),
		mpus:     make(map[string]*object.MultipartUpload),
		key2part: make(map[string][]*fPart),
	}
}

var _ plugin = (*fPlugin)(nil)

type fPart struct {
	num  int
	body []byte
}

type fPlugin struct {
	endpoint string
	pool     msg.BytesPool
	objects  map[string][]byte
	mpus     map[string]*object.MultipartUpload // simple implementation
	key2part map[string][]*fPart
}

var (
	fRid    = "fake-request-id"
	fSC     = "fake-storage-class"
	fLimits = &object.Limits{
		IsSupportMultipartUpload: false,
		IsSupportUploadPartCopy:  true,
		MinPartSize:              2,
		MaxPartSize:              10,
		MaxPartCount:             2,
	}
)

func (t *fPlugin) init(in any) (any, error) {
	if in == nil {
		return nil, errors.New("init input cannot be nil")
	}
	t.endpoint = in.(*initIn).endpoint
	return nil, nil
}

func (t *fPlugin) str(any) (any, error) {
	return t.endpoint, nil
}

func (t *fPlugin) limits(any) (any, error) {
	return fLimits, nil
}

type fGetReader struct {
	*bytes.Reader
}

func (r *fGetReader) Close() error { return nil }

func (t *fPlugin) get(in any) (any, error) {
	if in == nil {
		return &getOut{sc: fSC, rid: fRid}, errors.New("get input cannot be nil")
	}
	gIn, ok := in.(*getIn)
	if !ok {
		return &getOut{sc: fSC, rid: fRid}, errors.New("invalid input type")
	}
	data, ok := t.objects[gIn.key]
	if !ok {
		return &getOut{sc: fSC, rid: fRid}, errors.New("object not found")
	}
	if gIn.limit > 0 {
		data = data[gIn.offset : gIn.offset+gIn.limit]
	} else {
		data = data[gIn.offset:]
	}
	return &getOut{
		rc:  &fGetReader{bytes.NewReader(data)},
		rid: fRid,
		sc:  fSC,
	}, nil
}

func (t *fPlugin) put(in any) (any, error) {
	if in == nil {
		return nil, errors.New("put input cannot be nil")
	}
	tIn, ok := in.(*putIn)
	if !ok {
		return nil, errors.New("invalid input type")
	}
	_, ok = t.objects[tIn.key]
	if ok {
		logger.Debugf("overwrite %s", tIn.key)
	}
	t.objects[tIn.key] = make([]byte, tIn.reader.N)
	if _, err := io.ReadFull(tIn.reader, t.objects[tIn.key]); err != nil {
		return nil, errors.Wrapf(err, "failed to read put data")
	}
	return &putOut{
		rid: fRid,
		sc:  fSC,
	}, nil
}

func (t *fPlugin) create(in any) (any, error) {
	return nil, nil
}

func (t *fPlugin) delete(in any) (any, error) {
	key, ok := in.(string)
	if !ok {
		return &delOut{rid: fRid}, errors.New("invalid input type")
	}
	delete(t.objects, key)
	return &delOut{rid: fRid}, nil
}

func (t *fPlugin) copy(in any) (any, error) {
	cIn, ok := in.(*copyIn)
	if !ok {
		return nil, errors.New("invalid input type")
	}

	out, err := t.get(&getIn{key: cIn.src})
	if err != nil {
		return nil, err
	}
	_, err = t.put(&putIn{
		key:    cIn.dst,
		reader: &io.LimitedReader{R: out.(*getOut).rc, N: int64(out.(*getOut).rc.Len())},
	})
	return nil, err
}

func (t *fPlugin) head(in any) (any, error) {
	key, ok := in.(string)
	if !ok {
		return nil, errors.New("invalid input type")
	}

	info, ok := t.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}

	return &headOut{
		obj: obj{
			size: int64(len(info)),
			sc:   fSC,
		},
	}, nil
}

func (t *fPlugin) setStorageClass(in any) (any, error) {
	key, ok := in.(string)
	if !ok {
		return nil, errors.New("invalid input type")
	}
	fSC = key
	return nil, nil
}

func (t *fPlugin) list(in any) (any, error) {
	lIn, ok := in.(*listIn)
	if !ok {
		return nil, errors.New("invalid input type")
	}
	if lIn.limit <= 0 {
		return nil, errors.New("limit must be greater than 0")
	}
	var objects []obj
	cnt := 0
	for key, data := range t.objects {
		if int64(len(objects)) >= lIn.limit {
			break
		}
		cnt++
		if lIn.prefix != "" && !strings.HasPrefix(key, lIn.prefix) {
			continue
		}
		if lIn.startAfter != "" && key <= lIn.startAfter {
			continue
		}
		objects = append(objects, obj{
			key:  key,
			size: int64(len(data)),
			sc:   fSC,
		})
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].key < objects[j].key
	})
	var next string
	if len(objects) > 0 {
		next = objects[len(objects)-1].key
	}
	return &listOut{
		objects:     objects,
		nextMarker:  next,
		isTruncated: cnt < len(t.objects),
	}, nil
}

func (t *fPlugin) createMultipartUpload(in any) (any, error) {
	key := in.(string)
	t.mpus[key] = &object.MultipartUpload{
		MinPartSize: fLimits.MinPartSize,
		MaxCount:    fLimits.MaxPartCount,
		UploadID:    time.Now().Format("150405"),
	}
	return t.mpus[key], nil
}

func (t *fPlugin) uploadPart(in any) (any, error) {
	upIn := in.(*uloadPartIn)
	data := make([]byte, upIn.body.N)
	if _, err := io.ReadFull(upIn.body, data); err != nil { // ensure we consume all data
		return nil, errors.Wrapf(err, "failed to read upload part data")
	}

	mpu, ok := t.mpus[upIn.key]
	if !ok {
		return nil, errors.New("multipart upload not found")
	} else if mpu.UploadID != upIn.uploadID {
		return nil, errors.New("invalid upload ID")
	} else if len(t.key2part[upIn.key]) >= mpu.MaxCount {
		return nil, errors.Errorf("exceed max part count %d", mpu.MaxCount)
	}
	t.key2part[upIn.key] = append(t.key2part[upIn.key], &fPart{
		num:  upIn.num,
		body: data,
	})
	md5Bytes := md5.Sum(data)
	return &object.Part{
		Num:  upIn.num,
		Size: len(data),
		ETag: hex.EncodeToString(md5Bytes[:]),
	}, nil
}

func (t *fPlugin) uploadPartCopy(in any) (any, error) {
	upcIn := in.(*uploadPartCopyIn)
	srcObj, ok := t.objects[upcIn.srcKey]
	if !ok {
		return nil, errors.New("source object not found")
	}
	if upcIn.off < 0 || upcIn.off+upcIn.size > int64(len(srcObj)) {
		return nil, errors.New("invalid offset or size for upload part copy")
	}
	data := srcObj[upcIn.off : upcIn.off+upcIn.size]
	if len(data) < fLimits.MinPartSize || len(data) > int(fLimits.MaxPartSize) {
		return nil, errors.Errorf("part size must be between %d and %d bytes", fLimits.MinPartSize, fLimits.MaxPartSize)
	}

	if _, ok := t.mpus[upcIn.dstKey]; !ok {
		return nil, errors.New("multipart upload not found")
	}
	if t.mpus[upcIn.dstKey].UploadID != upcIn.uploadID {
		return nil, errors.New("invalid upload ID")
	}
	t.key2part[upcIn.dstKey] = append(t.key2part[upcIn.dstKey], &fPart{
		num:  upcIn.num,
		body: data,
	})
	md5Bytes := md5.Sum(data)
	return &object.Part{
		Num:  upcIn.num,
		Size: len(data),
		ETag: hex.EncodeToString(md5Bytes[:]),
	}, nil
}

func (t *fPlugin) abortMultipartUpload(in any) (any, error) {
	aIn := in.(*abortMPUIn)
	if _, ok := t.mpus[aIn.key]; !ok {
		return nil, errors.New("multipart upload not found")
	}
	if t.mpus[aIn.key].UploadID != aIn.uploadID {
		return nil, errors.New("invalid upload ID")
	}
	delete(t.mpus, aIn.key)
	delete(t.key2part, aIn.key)
	return nil, nil
}

func (t *fPlugin) completeMultipartUpload(in any) (any, error) {
	cIn := in.(*completeMPUIn)
	if _, ok := t.mpus[cIn.key]; !ok {
		return nil, errors.New("multipart upload not found")
	}
	if t.mpus[cIn.key].UploadID != cIn.uploadID {
		return nil, errors.New("invalid upload ID")
	}
	parts := t.key2part[cIn.key]
	partM := make(map[int]*fPart)
	total := 0
	for _, p := range parts {
		partM[p.num] = p
		total += len(p.body)
	}
	for _, p := range cIn.parts {
		if part, ok := partM[p.Num]; ok {
			if len(part.body) != p.Size {
				return nil, errors.Errorf("part size mismatch for part %d: expected %d, got %d", p.Num, p.Size, len(part.body))
			}
			md5Bytes := md5.Sum(part.body)
			if p.ETag != hex.EncodeToString(md5Bytes[:]) {
				return nil, errors.Errorf("ETag mismatch for part %d: expected %s, got %s", p.Num, p.ETag, hex.EncodeToString(md5Bytes[:]))
			}
		} else {
			return nil, errors.Errorf("part %d not found in multipart upload", p.Num)
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].num < parts[j].num
	})
	t.objects[cIn.key] = make([]byte, total)
	offset := 0
	for _, p := range parts {
		copy(t.objects[cIn.key][offset:], p.body)
		offset += len(p.body)
	}
	delete(t.mpus, cIn.key)
	delete(t.key2part, cIn.key)
	return nil, nil
}

func (t *fPlugin) listMultipartUploads(in any) (any, error) {
	key := in.(string)
	if _, ok := t.mpus[key]; !ok {
		return nil, errors.New("multipart upload not found")
	}
	parts := t.key2part[key]
	pendingParts := make([]*object.PendingPart, len(parts))
	for i := range parts {
		pendingParts[i] = &object.PendingPart{
			Key:      key,
			UploadID: t.mpus[key].UploadID,
			Created:  time.Now(),
		}
	}
	return &listMPUOut{
		nextMarker: "",
		parts:      pendingParts,
	}, nil
}
