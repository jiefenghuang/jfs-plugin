package pkg

import (
	"bytes"
	"io"
	"net"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/pkg/errors"
)

var _ object.ObjectStorage = (*Client)(nil)
var _ object.SupportStorageClass = (*Client)(nil)

func (c *Client) SetStorageClass(sc string) error {
	return c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen + 2 + len(sc))
		defer c.pool.Put(buff)
		m := newEncMsg(buff, 2+len(sc), cmdSetSC)
		m.putString(sc)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write SetStorageClass request")
		}
		_, err = c.readResp(conn, buff[:headerLen], cmdSetSC)
		return err
	})
}

func (c *Client) AbortUpload(key string, uploadID string) {
	if err := c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen + 2 + len(key) + 2 + len(uploadID))
		defer c.pool.Put(buff)
		m := newEncMsg(buff, 2+len(key)+2+len(uploadID), cmdAbortMPU)
		m.putString(key)
		m.putString(uploadID)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write AbortUpload request")
		}
		_, err = c.readResp(conn, buff[:headerLen], cmdAbortMPU)
		return err
	}); err != nil {
		c.Logger.Error("failed to call AbortUpload: %v", err)
	}
}

func (c *Client) CompleteUpload(key string, uploadID string, parts []*object.Part) error {
	return c.call(func(conn net.Conn) error {
		bLen := 2 + len(key) + 2 + len(uploadID) + 4 // last 4 bytes is a placeholder for next batch length
		buff := c.pool.Get(4 + 1<<20)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bLen, cmdCompleteMPU)
		m.putString(key)
		m.putString(uploadID)

		off := m.Offset()
		batchLen := 0
		m.Put32(0)

		var err error
		if len(parts) == 0 {
			if _, err = conn.Write(m.Bytes()[:m.Offset()]); err != nil {
				return errors.Wrap(err, "failed to write CompleteUpload request with no parts")
			}
		} else {
			var partLen int
			for _, part := range parts {
				partLen = 2 + len(part.ETag) + 4 + 4
				if m.Left() < partLen {
					c.Logger.Debugf("batch length %d exceeds buffer size, flushing", m.Offset())
					data := m.Bytes()[:m.Offset()]
					m.Seek(off)
					m.Put32(uint32(batchLen))
					if _, err := conn.Write(data); err != nil {
						return errors.Wrap(err, "failed to write CompleteUpload batch")
					}
					m.SetBytes(buff)
					off = m.Offset()
					batchLen = 0
					m.Put32(0)
				}
				m.Put32(uint32(part.Num))
				m.Put32(uint32(part.Size))
				m.putString(part.ETag)
				batchLen += partLen
			}

			if batchLen > 0 {
				m.Put32(0)
				data := m.Bytes()[:m.Offset()]
				m.Seek(off)
				m.Put32(uint32(batchLen))
				if _, err = conn.Write(data); err != nil {
					return errors.Wrap(err, "failed to write CompleteUpload final batch")
				}
			}
		}

		_, err = c.readResp(conn, buff[:headerLen], cmdCompleteMPU)
		return err
	})
}

func (c *Client) Copy(dst string, src string) error {
	return c.call(func(conn net.Conn) (err error) {
		bLen := 4 + len(dst) + len(src)
		buff := c.pool.Get(headerLen + bLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bLen, cmdCopy)
		m.putString(dst)
		m.putString(src)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Copy request")
		}

		_, err = c.readResp(conn, buff, cmdCopy)
		return err
	})
}

// readResp need put buff back to pool if msg != nil
func (c *Client) readResp(conn net.Conn, hBuff []byte, cmd byte) (*msg, error) {
	var err error
	if hBuff == nil || len(hBuff) != headerLen {
		hBuff = c.pool.Get(headerLen)
		defer c.pool.Put(hBuff)
	}
	if _, err = io.ReadFull(conn, hBuff); err != nil {
		return nil, errors.Wrapf(err, "cmd %s failed to read response header", cmd2Name[cmd])
	}
	m := newMsg(hBuff)
	bodyLen, rCmd := m.getHeader()
	if rCmd != cmd {
		return nil, errors.Errorf("cmd %s got unexpected command in response: %s", cmd2Name[cmd], cmd2Name[rCmd])
	}
	if bodyLen == 0 {
		return nil, nil
	}
	buff := c.pool.Get(int(bodyLen))
	if _, err = io.ReadFull(conn, buff); err != nil {
		c.pool.Put(buff)
		return nil, errors.Wrapf(err, "cmd %s failed to read response body", cmd2Name[cmd])
	}
	m.SetBytes(buff)
	errMsg := m.getString()
	if errMsg != "" {
		err = errors.Errorf("cmd %s failed: %s", cmd2Name[cmd], errMsg)
	}
	if !m.HasMore() {
		c.pool.Put(buff)
		return nil, err
	}
	return m, err
}

func (c *Client) Create() error {
	return c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen)
		m := newEncMsg(buff, 0, cmdCreate)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Create request")
		}
		_, err = c.readResp(conn, buff, cmdCreate)
		return err
	})
}

func (c *Client) CreateMultipartUpload(key string) (*object.MultipartUpload, error) {
	var mpu *object.MultipartUpload
	err := c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen + 2 + len(key))
		defer c.pool.Put(buff)
		m := newEncMsg(buff, 2+len(key), cmdCreateMPU)
		m.putString(key)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write CreateMultipartUpload request")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdCreateMPU)
		if err != nil {
			return err
		}
		defer c.pool.Put(m.Bytes())
		mpu = &object.MultipartUpload{
			MinPartSize: int(m.Get32()),
			MaxCount:    int(m.Get32()),
			UploadID:    m.getString(),
		}
		return nil
	})
	return mpu, err
}

func (c *Client) Delete(key string, getters ...object.AttrGetter) error {
	return c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen + 2 + len(key))
		defer c.pool.Put(buff)
		m := newEncMsg(buff, 2+len(key), cmdDel)
		m.putString(key)
		if _, err := conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Delete request")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdDel)
		if m != nil {
			attrs := object.ApplyGetters(getters...)
			attrs.SetStorageClass(m.getString())
			c.pool.Put(m.Bytes())
		}
		return err
	})
}

type buffReader struct {
	*bytes.Buffer
	pool     *bufferPool
	fullData []byte
}

func newBuffReader(data []byte, fullData []byte, pool *bufferPool) *buffReader {
	return &buffReader{
		Buffer:   bytes.NewBuffer(data),
		fullData: fullData,
		pool:     pool,
	}
}

func (b *buffReader) Close() error {
	b.pool.Put(b.fullData)
	return nil
}

var _ io.ReadCloser = (*buffReader)(nil)

func (c *Client) Get(key string, off int64, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	var rc io.ReadCloser
	err := c.call(func(conn net.Conn) (err error) {
		bLen := 2 + len(key) + 16
		buff := c.pool.Get(headerLen + bLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bLen, cmdGet)
		m.putString(key)
		m.Put64(uint64(off))
		attrs := object.ApplyGetters(getters...)
		if limit <= 0 && attrs.GetRequestSize() > 0 {
			m.Put64(uint64(attrs.GetRequestSize()))
		} else {
			m.Put64(uint64(limit))
		}

		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Get header")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdGet)
		if m != nil {
			rid, sc := m.getString(), m.getString()
			attrs.SetRequestID(rid).SetStorageClass(sc)
		}
		if err != nil {
			c.pool.Put(m.Bytes())
			return err
		}
		rc = newBuffReader(m.Buffer.Buffer(), m.Bytes(), c.pool)
		return nil
	})
	return rc, err
}

func (c *Client) Head(key string) (object.Object, error) {
	var o object.Object
	err := c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen + 2 + len(key))
		defer c.pool.Put(buff)
		m := newEncMsg(buff, 2+len(key), cmdHead)
		m.putString(key)

		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Head request")
		}

		m, err = c.readResp(conn, buff, cmdHead)
		if err != nil {
			return err
		}
		o = &obj{
			key:   key,
			sc:    m.getString(),
			size:  int64(m.Get64()),
			mtime: time.Unix(0, int64(m.Get64())),
			isDir: m.getBool(),
		}
		c.pool.Put(m.Bytes())
		return nil
	})
	return o, err
}

func (c *Client) Limits() object.Limits {
	var limit object.Limits
	if err := c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, 0, cmdLimits)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Limits request")
		}

		m, err = c.readResp(conn, buff, cmdLimits)
		limit.IsSupportMultipartUpload = m.getBool()
		limit.IsSupportUploadPartCopy = m.getBool()
		limit.MinPartSize = int(m.Get64())
		limit.MaxPartSize = int64(m.Get64())
		limit.MaxPartCount = int(m.Get64())
		c.pool.Put(m.Bytes())
		return err
	}); err != nil {
		c.Logger.Errorf("failed to call Limits: %v", err)
	}
	return limit
}

func (c *Client) List(prefix string, startAfter string, token string, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	var objs []object.Object
	isTruncated := false
	nextMarker := ""
	err := c.call(func(conn net.Conn) (err error) {
		bodyLen := 8 + len(prefix) + len(startAfter) + len(token) + len(delimiter) + 9
		buff := c.pool.Get(headerLen + bodyLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bodyLen, cmdList)
		m.putString(prefix)
		m.putString(startAfter)
		m.putString(token)
		m.putString(delimiter)
		m.Put64(uint64(limit))
		m.putBool(followLink)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write List request")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdList)
		if err != nil {
			return err
		}
		defer c.pool.Put(m.Bytes())
		isTruncated = m.getBool()
		nextMarker = m.getString()
		batchLen := m.Get32()

		batchBuff := c.pool.Get(1<<20 + 4)
		defer c.pool.Put(batchBuff)
		for {
			if batchLen == 0 {
				break
			}
			if _, err = io.ReadFull(conn, batchBuff[:batchLen+4]); err != nil {
				return errors.Wrapf(err, "cmd %s failed to read response body", cmd2Name[cmdList])
			}
			m.SetBytes(batchBuff[:batchLen])
			for m.HasMore() {
				objs = append(objs, &obj{
					key:   m.getString(),
					sc:    m.getString(),
					size:  int64(m.Get64()),
					mtime: time.Unix(0, int64(m.Get64())),
					isDir: m.getBool(),
				})
			}
			m.SetBytes(batchBuff[batchLen : batchLen+4])
			batchLen = m.Get32()
		}
		return nil
	})
	return objs, isTruncated, nextMarker, err
}

func (c *Client) ListAll(prefix string, marker string, followLink bool) (<-chan object.Object, error) {
	return nil, utils.ENOTSUP
}

func (c *Client) ListUploads(marker string) ([]*object.PendingPart, string, error) {
	var parts []*object.PendingPart
	var nextMarker string
	err := c.call(func(conn net.Conn) (err error) {
		bodyLen := 2 + len(marker)
		buff := c.pool.Get(headerLen + bodyLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bodyLen, cmdListMPU)
		m.putString(marker)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write ListUploads request")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdListMPU)
		if err != nil {
			return err
		}
		defer c.pool.Put(m.Bytes())
		nextMarker = m.getString()
		batchLen := m.Get32()

		batchBuff := c.pool.Get(1<<20 + 4)
		defer c.pool.Put(batchBuff)
		for {
			if batchLen == 0 {
				break
			}
			if _, err = io.ReadFull(conn, batchBuff[:batchLen+4]); err != nil {
				return errors.Wrapf(err, "cmd %s failed to read response batch", cmd2Name[cmdListMPU])
			}
			m.SetBytes(batchBuff[:batchLen])
			for m.HasMore() {
				parts = append(parts, &object.PendingPart{
					Key:      m.getString(),
					UploadID: m.getString(),
					Created:  time.Unix(0, int64(m.Get64())),
				})
			}
			m.SetBytes(batchBuff[batchLen : batchLen+4])
			batchLen = m.Get32()
		}
		return nil
	})
	return parts, nextMarker, err
}

func (c *Client) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	return c.call(func(conn net.Conn) error {
		l, ok := in.(lener)
		if !ok {
			return errors.New("input reader does not implement Len() interface")
		}

		bLen := 2 + len(key) + 4 // 4 for length of data
		buff := c.pool.Get(headerLen + bLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bLen, cmdPut)
		m.putString(key)
		m.Put32(uint32(l.Len()))

		var err error
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write Put header")
		}
		if _, err = io.Copy(conn, in); err != nil {
			return errors.Wrap(err, "failed to write Put body")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdPut)
		if m != nil {
			rid, sc := m.getString(), m.getString()
			attrs := object.ApplyGetters(getters...)
			attrs.SetRequestID(rid).SetStorageClass(sc)
			c.pool.Put(m.Bytes())
		}
		return err
	})
}

func (c *Client) String() string {
	str := ""
	if err := c.call(func(conn net.Conn) (err error) {
		buff := c.pool.Get(headerLen)
		m := newEncMsg(buff, 0, cmdStr)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write String request")
		}

		m, err = c.readResp(conn, buff, cmdStr)
		if m != nil {
			str = m.getString()
			c.pool.Put(m.Bytes())
		}
		return err
	}); err != nil {
		c.Logger.Errorf("failed to call client.String: %v", err)
	}
	return str
}

func (c *Client) UploadPart(key string, uploadID string, num int, body []byte) (*object.Part, error) {
	var part *object.Part
	err := c.call(func(conn net.Conn) (err error) {
		bLen := 2 + len(key) + 2 + len(uploadID) + 4 + 4
		buff := c.pool.Get(headerLen + bLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bLen, cmdUploadPart)
		m.putString(key)
		m.putString(uploadID)
		m.Put32(uint32(num))
		m.Put32(uint32(len(body)))

		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write UploadPart request")
		}
		if _, err = conn.Write(body); err != nil {
			return errors.Wrap(err, "failed to write UploadPart body")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdUploadPart)
		if err != nil {
			return err
		}
		defer c.pool.Put(m.Bytes())
		part = &object.Part{
			Num:  int(m.Get32()),
			Size: int(m.Get32()),
			ETag: m.getString(),
		}
		return nil
	})
	return part, err
}

func (c *Client) UploadPartCopy(key string, uploadID string, num int, srcKey string, off int64, size int64) (*object.Part, error) {
	var part *object.Part
	err := c.call(func(conn net.Conn) (err error) {
		bLen := 2 + len(key) + 2 + len(uploadID) + 4 + 2 + len(srcKey) + 8 + 8
		buff := c.pool.Get(headerLen + bLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bLen, cmdUploadPartCopy)
		m.putString(key)
		m.putString(uploadID)
		m.Put32(uint32(num))
		m.putString(srcKey)
		m.Put64(uint64(off))
		m.Put64(uint64(size))

		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write UploadPartCopy request")
		}

		m, err = c.readResp(conn, buff[:headerLen], cmdUploadPartCopy)
		if err != nil {
			return err
		}
		defer c.pool.Put(m.Bytes())
		part = &object.Part{
			Num:  int(m.Get32()),
			Size: int(m.Get32()),
			ETag: m.getString(),
		}
		return nil
	})
	return part, err
}

func (c *Client) Init(endpoint, accesskey, secretkey, token string) error {
	return c.call(func(conn net.Conn) (err error) {
		bodyLen := 8 + len(endpoint) + len(accesskey) + len(secretkey) + len(token)
		buff := c.pool.Get(headerLen + bodyLen)
		defer c.pool.Put(buff)
		m := newEncMsg(buff, bodyLen, cmdInit)
		m.putString(endpoint)
		m.putString(accesskey)
		m.putString(secretkey)
		m.putString(token)
		if _, err = conn.Write(m.Bytes()); err != nil {
			return errors.Wrap(err, "failed to write String request")
		}
		_, err = c.readResp(conn, buff[:headerLen], cmdInit)
		return err
	})
}
