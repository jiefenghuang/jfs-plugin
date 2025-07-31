package server

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
	mrand "math/rand"
	"os"
	"path"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jiefenghuang/jfs-plugin/pkg/msg"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func newTestPlugin(pool msg.BytesPool) plugin {
	return &tPlugin{
		pool: pool,
	}
}

var _ plugin = (*tPlugin)(nil)

type part struct {
	num  int
	body []byte
}

type tPlugin struct {
	endpoint string
	pool     msg.BytesPool
	objects  map[string][]byte
	mpus     map[string]*object.MultipartUpload // simple implementation
	key2part map[string][]*fPart
}

var (
	rid    = "fake-request-id"
	sc     = "fake-storage-class"
	limits = &object.Limits{
		IsSupportMultipartUpload: false,
		IsSupportUploadPartCopy:  true,
		MinPartSize:              2,
		MaxPartSize:              10,
		MaxPartCount:             2,
	}
)

func (t *tPlugin) init(in any) (any, error) {
	if in == nil {
		return nil, errors.New("init input cannot be nil")
	}
	t.endpoint = in.(*initIn).endpoint
	t.mpus = make(map[string]*object.MultipartUpload)
	t.key2part = make(map[string][]*fPart)
	return nil, nil
}

func (t *tPlugin) str(any) (any, error) {
	return t.endpoint, nil
}

func (t *tPlugin) limits(any) (any, error) {
	return fLimits, nil
}

type getReader struct {
	*bytes.Reader
}

func (r *getReader) Close() error { return nil }

func (t *tPlugin) get(in any) (any, error) {
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

func (t *tPlugin) put(in any) (any, error) {
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

func (t *tPlugin) create(in any) (any, error) {
	t.objects = make(map[string][]byte)
	return nil, nil
}

func (t *tPlugin) delete(in any) (any, error) {
	key, ok := in.(string)
	if !ok {
		return &delOut{rid: fRid}, errors.New("invalid input type")
	}
	delete(t.objects, key)
	return &delOut{rid: fRid}, nil
}

func (t *tPlugin) copy(in any) (any, error) {
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

func (t *tPlugin) head(in any) (any, error) {
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

func (t *tPlugin) setStorageClass(in any) (any, error) {
	key, ok := in.(string)
	if !ok {
		return nil, errors.New("invalid input type")
	}
	fSC = key
	return nil, nil
}

func (t *tPlugin) list(in any) (any, error) {
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

func (t *tPlugin) createMultipartUpload(in any) (any, error) {
	key := in.(string)
	t.mpus[key] = &object.MultipartUpload{
		MinPartSize: fLimits.MinPartSize,
		MaxCount:    fLimits.MaxPartCount,
		UploadID:    time.Now().Format("150405"),
	}
	return t.mpus[key], nil
}

func (t *tPlugin) uploadPart(in any) (any, error) {
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

func (t *tPlugin) uploadPartCopy(in any) (any, error) {
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

func (t *tPlugin) abortMultipartUpload(in any) (any, error) {
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

func (t *tPlugin) completeMultipartUpload(in any) (any, error) {
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

func (t *tPlugin) listMultipartUploads(in any) (any, error) {
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

func TestPlugin(t *testing.T) {
	utils.SetLogLevel(logrus.DebugLevel)
	url := "tcp://localhost:8080"

	svr, err := NewServer(&SvrOptions{URL: url})
	require.Nil(t, err)
	svr.setPlugin(newTestPlugin(svr.pool))
	done := make(chan struct{})
	go svr.Start(done)
	defer svr.close()
	<-done

	cli, err := object.NewPluginClient(&object.PluginOptions{
		Version: "1.4.0",
		URL:     url,
		Logger:  logger,
	})
	require.Nil(t, err)
	defer cli.Close()

	p := path.Join(os.TempDir(), "jfs-plugin-data")
	t.Run("Init", func(t *testing.T) {
		require.Nil(t, cli.Init(p, "", "", ""))
	})
	defer os.RemoveAll(p)

	t.Run("Create", func(t *testing.T) {
		require.Nil(t, cli.Create())
	})
	t.Run("String", func(t *testing.T) {
		require.Equal(t, p, cli.String())
	})
	t.Run("Limits", func(t *testing.T) {
		require.True(t, reflect.DeepEqual(*fLimits, cli.Limits()))
	})

	num := 2000
	bigNum := 5
	keys, vals := make([]string, num), make([][]byte, num)
	t.Run("Put", func(t *testing.T) {
		for i := 0; i < num; i++ {
			keys[i] = randString(mrand.Intn(1024) + 1)
			if i < bigNum {
				vals[i] = randBytes(mrand.Intn(4<<20) + 1)
			} else {
				vals[i] = []byte(randString(mrand.Intn(1024) + 1))
			}
			require.Nil(t, cli.Put(keys[i], bytes.NewReader(vals[i])))
		}
	})
	t.Run("Get", func(t *testing.T) {
		for i := 0; i < num; i++ {
			r, err := cli.Get(keys[i], 0, -1)
			require.Nil(t, err)
			data, err := io.ReadAll(r)
			require.Nil(t, err)
			require.Equal(t, vals[i], data)
			r.Close()
		}
	})
	t.Run("Delete", func(t *testing.T) {
		require.Nil(t, cli.Delete(keys[0]))
		_, err = cli.Get(keys[0], 0, -1)
		require.Contains(t, err.Error(), "object not found")
	})
	t.Run("Copy", func(t *testing.T) {
		require.Nil(t, cli.Copy(keys[0], keys[1]))
		r1, err := cli.Get(keys[0], 0, -1)
		require.Nil(t, err)
		r2, err := cli.Get(keys[1], 0, -1)
		require.Nil(t, err)
		data1, err := io.ReadAll(r1)
		require.Nil(t, err)
		data2, err := io.ReadAll(r2)
		require.Nil(t, err)
		require.Equal(t, data1, data2)
		r1.Close()
		r2.Close()
	})
	t.Run("Head", func(t *testing.T) {
		obj, err := cli.Head(keys[0])
		require.Nil(t, err)
		require.Equal(t, keys[0], obj.Key())
		obj2, err := cli.Head(keys[1])
		require.Nil(t, err)
		require.Equal(t, keys[1], obj2.Key())

		require.Equal(t, obj.Size(), obj2.Size())
		require.Equal(t, obj.StorageClass(), obj2.StorageClass())
		require.Equal(t, obj.IsDir(), obj2.IsDir())
	})
	t.Run("SetStorageClass", func(t *testing.T) {
		nsc := "new-sc"
		require.Nil(t, cli.SetStorageClass(nsc))
		obj, err := cli.Head(keys[0])
		require.Nil(t, err)
		require.Equal(t, nsc, obj.StorageClass())
	})
	t.Run("List", func(t *testing.T) {
		objs, isTruncated, nextMarker, err := cli.List("", "", "", "", int64(num), false)
		require.Nil(t, err)
		require.False(t, isTruncated)
		require.Equal(t, slices.Max(keys), nextMarker)
		require.Equal(t, num, len(objs))

		_, _, _, err = cli.List("", "", "", "", -1, false)
		require.Contains(t, err.Error(), "limit must be greater than 0")

		objs, isTruncated, _, err = cli.List("", "", "", "", int64(num/10), false)
		require.Nil(t, err)
		require.True(t, isTruncated)
		require.LessOrEqual(t, len(objs), num/10)
	})
	t.Run("Multipart Upload", func(t *testing.T) {
		key := "test-mpu"
		mpu, err := cli.CreateMultipartUpload(key)
		require.Nil(t, err)
		require.Equal(t, fLimits.MinPartSize, mpu.MinPartSize)
		require.Equal(t, fLimits.MaxPartCount, mpu.MaxCount)
		require.NotEmpty(t, mpu.UploadID)

		_, err = cli.UploadPart(key, mpu.UploadID, 1, []byte("part1"))
		require.Nil(t, err)
		_, err = cli.UploadPart(key, mpu.UploadID, 2, []byte("part2"))
		require.Nil(t, err)

		parts, nextMarker, err := cli.ListUploads(key)
		require.Nil(t, err)
		require.Equal(t, "", nextMarker)
		require.Len(t, parts, 2)

		cli.AbortUpload(key, mpu.UploadID)
		_, _, err = cli.ListUploads(key)
		require.Contains(t, err.Error(), "multipart upload not found")

		// redo
		mpu, err = cli.CreateMultipartUpload(key)
		require.Nil(t, err)
		require.Equal(t, fLimits.MinPartSize, mpu.MinPartSize)
		require.Equal(t, fLimits.MaxPartCount, mpu.MaxCount)
		require.NotEmpty(t, mpu.UploadID)

		part1, err := cli.UploadPart(key, mpu.UploadID, 1, []byte("part1"))
		require.Nil(t, err)
		part2, err := cli.UploadPartCopy(key, mpu.UploadID, 2, keys[1], 0, 5)
		require.Nil(t, err)
		_, err = cli.UploadPart(key, mpu.UploadID, 3, []byte("part3"))
		require.Contains(t, err.Error(), "exceed max part count")

		err = cli.CompleteUpload(key, mpu.UploadID, []*object.Part{part1, part2})
		require.Nil(t, err)

		rc, err := cli.Get(key, 0, -1)
		require.Nil(t, err)
		defer rc.Close()
		data, err := io.ReadAll(rc)
		require.Nil(t, err)
		expectedData := make([]byte, 10)
		copy(expectedData[:5], []byte("part1"))
		copy(expectedData[5:], vals[1][:5])
		require.Equal(t, expectedData, data)
	})
}

func randString(size int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = letters[mrand.Intn(len(letters))]
	}
	return string(b)
}

func randBytes(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}
