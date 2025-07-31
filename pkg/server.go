package pkg

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/pkg/errors"
)

var logger = utils.GetLogger("jfs-plugin")

type SvrOptions struct {
	URL        string
	proto      string // "tcp" or "unix"
	addr       string
	BuffList   []int
	MinVersion *version.Semver
	MaxVersion *version.Semver
}

func (opt *SvrOptions) Check() error {
	proto, addr := SplitAddr(opt.URL)
	if proto == "" || addr == "" {
		return errors.Errorf("invalid address format %s, expected 'tcp://<addr>' or 'unix://<path>'", opt.URL)
	}
	opt.proto, opt.addr = proto, addr
	if err := checkProto(opt.proto); err != nil {
		return err
	}
	if opt.BuffList == nil {
		opt.BuffList = DefaultCliCapList
	}
	return nil
}

type server struct {
	*SvrOptions
	running  bool
	listener net.Listener
	enc      *svrEncoder
	dec      *svrDecoder
	pool     *bufferPool
	handlers map[byte]func(any) (any, error)
}

func NewServer(opt *SvrOptions) (*server, error) {
	if err := opt.Check(); err != nil {
		return nil, err
	}
	pool := newBufferPool(opt.BuffList)
	svr := &server{
		SvrOptions: opt,
		dec:        newSvrDecoder(pool),
		enc:        newSvrEncoder(pool),
		pool:       pool,
	}
	svr.setPlugin(newPlugin(pool))
	return svr, nil
}

func (s *server) setPlugin(pl plugin) {
	if pl == nil {
		logger.Errorf("plugin is nil, cannot set handlers")
		return
	}
	s.handlers = map[byte]func(any) (any, error){
		cmdPut:            pl.put,
		cmdGet:            pl.get,
		cmdStr:            pl.str,
		cmdLimits:         pl.limits,
		cmdInit:           pl.init,
		cmdCreate:         pl.create,
		cmdDel:            pl.delete,
		cmdCopy:           pl.copy,
		cmdHead:           pl.head,
		cmdSetSC:          pl.setStorageClass,
		cmdList:           pl.list,
		cmdCreateMPU:      pl.createMultipartUpload,
		cmdUploadPart:     pl.uploadPart,
		cmdUploadPartCopy: pl.uploadPartCopy,
		cmdCompleteMPU:    pl.completeMultipartUpload,
		cmdListMPU:        pl.listMultipartUploads,
		cmdAbortMPU:       pl.abortMultipartUpload,
	}
}

func (s *server) Start(done chan<- struct{}) error {
	if s.running {
		return errors.New("server is already running")
	}
	s.running = true
	var err error
	for i := 0; i < 3; i++ {
		s.listener, err = net.Listen(s.proto, s.addr)
		if err == nil {
			break
		}
		logger.Warnf("failed to listen on %s: %v, retrying...", s.URL, err)
		time.Sleep(time.Second * time.Duration(i+1))
	}

	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", s.URL)
	}

	logger.Infof("server listening on %s", s.listener.Addr())

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
		<-signalChan
		s.close()
	}()

	if done != nil {
		close(done)
	}
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			logger.Errorf("failed to accept connection: %v", err)
			continue
		}
		go s.serve(conn)
	}
}

func (s *server) close() {
	if err := s.listener.Close(); err != nil {
		logger.Errorf("failed closing listener: %v", err)
	}
	s.running = false
	logger.Info("server shutdown complete.")
}

func isAny(err error, targets ...error) bool {
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (s *server) serve(conn net.Conn) {
	defer conn.Close()
	logger.Debugf("accepted connection from %s", conn.RemoteAddr())

	if err := s.auth(conn); err != nil {
		logger.Errorf("init conn failed: %+v", err)
		return
	}
	var req request
	var resp response
	var err error
	for {
		if err = s.dec.decode(conn, &req); err != nil {
			if !isAny(err, io.EOF, net.ErrClosed) {
				logger.Errorf("failed to decode request: %v", err)
			}
			break
		}

		resp.out, err = s.handlers[req.cmd](req.in)
		resp.cmd = req.cmd
		if err == nil {
			resp.err = ""
		} else {
			resp.err = err.Error()
		}

		if err = s.enc.encoders[resp.cmd](conn, &resp); err != nil {
			logger.Errorf("failed to encode response for command %s: %v", cmd2Name[resp.cmd], err)
			break
		}
		req.clear()
	}
}

func (s *server) auth(conn net.Conn) error {
	defer conn.SetDeadline(time.Time{})

	var err error
	buff := s.pool.Get(headerLen)
	defer s.pool.Put(buff)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err = io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read CmdAuth header")
	}
	m := newMsg(buff)
	bLen, cmd := m.getHeader()
	if cmd != cmdAuth {
		return errors.Wrap(err, "the first request must be CmdAuth")
	}

	buff2 := s.pool.Get(int(bLen))
	defer s.pool.Put(buff2)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, buff2); err != nil {
		return errors.Wrap(err, "failed to read init payload")
	}
	m.SetBytes(buff2)
	ver := version.Parse(m.getString())
	if ver != nil {
		if s.MinVersion != nil {
			if ret, _ := version.CompareVersions(s.MinVersion, ver); ret < 0 {
				return errors.Errorf("JuiceFS version %s is lower than the minimum supported version %s", ver, s.MinVersion)
			}
		}
		if s.MaxVersion != nil {
			if ret, _ := version.CompareVersions(s.MaxVersion, ver); ret > 0 {
				return errors.Errorf("JuiceFS version %s is higher than the maximum supported version %s", ver, s.MaxVersion)
			}
		}
	}
	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	return s.enc.encoders[cmdAuth](conn, &response{
		cmd: cmdAuth,
		out: nil,
		err: "",
	})
}

type request struct {
	cmd       byte
	bodyLen   uint32
	in        any
	callbacks []func()
}

func (r *request) clear() {
	for _, cb := range r.callbacks {
		cb()
	}
	r.callbacks = nil
	r.cmd = cmdUnknown
	r.bodyLen = 0
	r.in = nil
}

func (r *request) withCallback(cb func()) *request {
	r.callbacks = append(r.callbacks, cb)
	return r
}

type response struct {
	cmd byte
	err string
	out any
}

type svrDecoder struct {
	decoders map[byte]func(net.Conn, *request) error
	pool     *bufferPool
}

func newSvrDecoder(buffPool *bufferPool) *svrDecoder {
	dec := &svrDecoder{
		pool: buffPool,
	}
	dec.decoders = map[byte]func(net.Conn, *request) error{
		cmdInit:           dec.decodeInitReq,
		cmdPut:            dec.decodePutReq,
		cmdGet:            dec.decodeGetReq,
		cmdStr:            dec.decodeEmptyReq,
		cmdLimits:         dec.decodeEmptyReq,
		cmdCreate:         dec.decodeEmptyReq,
		cmdDel:            dec.decodeDeleteReq,
		cmdCopy:           dec.decodeCopyReq,
		cmdHead:           dec.decodeHeadReq,
		cmdSetSC:          dec.decodeSetSCReq,
		cmdList:           dec.decodeListReq,
		cmdCreateMPU:      dec.decodeCreateMPUReq,
		cmdUploadPart:     dec.decodeUploadPartReq,
		cmdUploadPartCopy: dec.decodeUploadPartCopyReq,
		cmdAbortMPU:       dec.decodeAbortMPUReq,
		cmdCompleteMPU:    dec.decodeCompleteMPUReq,
		cmdListMPU:        dec.decodeListMPUReq,
	}
	return dec
}

func (dec *svrDecoder) decode(conn net.Conn, req *request) error {
	var err error
	buff := dec.pool.Get(headerLen)
	if _, err = io.ReadFull(conn, buff); err != nil {
		dec.pool.Put(buff)
		return errors.Wrap(err, "failed to read header")
	}
	req.bodyLen = binary.BigEndian.Uint32(buff[:4])
	req.cmd = buff[4]
	dec.pool.Put(buff)

	if req.cmd <= cmdUnknown || req.cmd >= cmdMax {
		return errors.Errorf("unknown command: %d", req.cmd)
	}
	return dec.decoders[req.cmd](conn, req)
}

func (dec *svrDecoder) decodeInitReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Init payload")
	}

	m := newMsg(buff)
	req.in = &initIn{
		endpoint:  m.getString(),
		accesskey: m.getString(),
		secretkey: m.getString(),
		token:     m.getString(),
	}
	return nil
}

func (dec *svrDecoder) decodeListMPUReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read ListMultipartUploads payload")
	}
	m := newMsg(buff)
	req.in = m.getString()
	return nil
}

func (dec *svrDecoder) decodeCompleteMPUReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read CompleteMultipartUpload payload")
	}
	m := newMsg(buff)
	in := &completeMPUIn{
		key:      m.getString(),
		uploadID: m.getString(),
	}
	batchLen := m.Get32()
	batchBuff := dec.pool.Get(1<<20 + 4)
	defer dec.pool.Put(batchBuff)
	for {
		if batchLen == 0 {
			break
		}
		if _, err := io.ReadFull(conn, batchBuff[:batchLen+4]); err != nil {
			return errors.Wrap(err, "failed to read CompleteMultipartUpload batch")
		}
		m.SetBytes(batchBuff[:batchLen])
		for m.HasMore() {
			in.parts = append(in.parts, &object.Part{
				Num:  int(m.Get32()),
				Size: int(m.Get32()),
				ETag: m.getString(),
			})
		}
		m.SetBytes(batchBuff[batchLen:])
		batchLen = m.Get32()
	}
	req.in = in
	return nil
}

func (dec *svrDecoder) decodeAbortMPUReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read AbortMultipartUpload payload")
	}
	m := newMsg(buff)
	req.in = &abortMPUIn{
		key:      m.getString(),
		uploadID: m.getString(),
	}
	return nil
}

func (dec *svrDecoder) decodeUploadPartCopyReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read UploadPartCopy payload")
	}

	m := newMsg(buff)
	req.in = &uploadPartCopyIn{
		dstKey:   m.getString(),
		uploadID: m.getString(),
		num:      int(m.Get32()),
		srcKey:   m.getString(),
		off:      int64(m.Get64()),
		size:     int64(m.Get64()),
	}
	return nil
}

func (dec *svrDecoder) decodeUploadPartReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read UploadPart payload")
	}

	m := newMsg(buff)
	req.in = &uloadPartIn{
		key:      m.getString(),
		uploadID: m.getString(),
		num:      int(m.Get32()),
		body: &io.LimitedReader{
			R: conn,
			N: int64(m.Get32()),
		},
	}
	return nil
}

func (dec *svrDecoder) decodeCreateMPUReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read CreateMultipartUpload payload")
	}
	m := newMsg(buff)
	req.in = m.getString()
	return nil
}

func (dec *svrDecoder) decodePutReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Put payload")
	}

	m := newMsg(buff)
	req.in = &putIn{
		key:    m.getString(),
		reader: &io.LimitedReader{R: conn, N: int64(m.Get32())},
	}
	return nil
}

func (dec *svrDecoder) decodeHeadReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Head payload")
	}
	m := newMsg(buff)
	req.in = m.getString()
	return nil
}

func (dec *svrDecoder) decodeCopyReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Copy payload")
	}
	m := newMsg(buff)
	req.in = &copyIn{dst: m.getString(), src: m.getString()}
	return nil
}

func (dec *svrDecoder) decodeListReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read List payload")
	}
	m := newMsg(buff)
	req.in = &listIn{
		prefix:     m.getString(),
		startAfter: m.getString(),
		token:      m.getString(),
		delimiter:  m.getString(),
		limit:      int64(m.Get64()),
		followLink: m.getBool(),
	}
	return nil
}

func (dec *svrDecoder) decodeSetSCReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read SetStorageClass payload")
	}
	m := newMsg(buff)
	req.in = m.getString()
	return nil
}

func (dec *svrDecoder) decodeDeleteReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Get payload")
	}
	m := newMsg(buff)
	req.in = m.getString()
	return nil
}

func (dec *svrDecoder) decodeGetReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Get payload")
	}
	m := newMsg(buff)
	req.in = &getIn{
		key:    m.getString(),
		offset: int64(m.Get64()),
		limit:  int64(m.Get64()),
	}
	return nil
}

func (dec *svrDecoder) decodeEmptyReq(conn net.Conn, req *request) error { return nil }

type svrEncoder struct {
	pool     *bufferPool
	encoders map[byte]func(net.Conn, *response) error
}

func newSvrEncoder(buffPool *bufferPool) *svrEncoder {
	enc := &svrEncoder{pool: buffPool}
	enc.encoders = map[byte]func(net.Conn, *response) error{
		cmdAuth:           enc.encodeSimpleResp,
		cmdPut:            enc.encodePutResp,
		cmdGet:            enc.encodeGetResp,
		cmdStr:            enc.encodeStrResp,
		cmdLimits:         enc.encodeLimitsResp,
		cmdInit:           enc.encodeSimpleResp,
		cmdCreate:         enc.encodeSimpleResp,
		cmdDel:            enc.encodeDelResp,
		cmdCopy:           enc.encodeSimpleResp,
		cmdHead:           enc.encodeHeadResp,
		cmdSetSC:          enc.encodeSimpleResp,
		cmdList:           enc.encodeListResp,
		cmdCreateMPU:      enc.encodeCreateMPUResp,
		cmdUploadPart:     enc.encodeUploadPartResp,
		cmdUploadPartCopy: enc.encodeUploadPartResp,
		cmdAbortMPU:       enc.encodeSimpleResp,
		cmdCompleteMPU:    enc.encodeSimpleResp,
		cmdListMPU:        enc.encodeListMPUResp,
	}
	return enc
}

func (enc *svrEncoder) encodeListMPUResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	out := resp.out.(*listMPUOut)
	bodyLen := 2 + len(resp.err) + 2 + len(out.nextMarker) + 4 // 4 is next batch length
	buff := enc.pool.Get(4 + 1<<20)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.putString(out.nextMarker)

	off := m.Offset()
	batchLen := 0
	m.Put32(0)

	var err error
	if len(out.parts) == 0 {
		_, err = conn.Write(m.Bytes())
		return err
	}

	var partLen int
	for _, part := range out.parts {
		partLen = 2 + len(part.Key) + 2 + len(part.UploadID) + 8
		if m.Left() < partLen {
			logger.Debugf("ListMultipartUploads response buffer is full, sending %d bytes", m.Offset())
			data := m.Bytes()[:m.Offset()]
			m.Seek(off)
			m.Put32(uint32(batchLen))
			if _, err := conn.Write(data); err != nil {
				return errors.Wrap(err, "failed to write ListMultipartUploads batch")
			}
			m.SetBytes(buff)
			off, batchLen = 0, 0
			m.Put32(0)
		}
		m.putString(part.Key)
		m.putString(part.UploadID)
		m.Put64(uint64(part.Created.UnixNano()))
		batchLen += partLen
	}

	if batchLen > 0 {
		m.Put32(0) // end marker
		data := m.Bytes()[:m.Offset()]
		m.Seek(off)
		m.Put32(uint32(batchLen))
		if _, err := conn.Write(data); err != nil {
			return errors.Wrap(err, "failed to write ListMultipartUploads last batch")
		}
	}

	return nil
}

func (enc *svrEncoder) encodeUploadPartResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	out := resp.out.(*object.Part)
	bodyLen := 2 + len(resp.err) + 4 + 4 + 2 + len(out.ETag)
	buff := enc.pool.Get(headerLen + bodyLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.Put32(uint32(out.Num))
	m.Put32(uint32(out.Size))
	m.putString(out.ETag)
	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write UploadPart response")
	}
	return nil
}

func (enc *svrEncoder) encodeCreateMPUResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	out := resp.out.(*object.MultipartUpload)
	bodyLen := 2 + len(resp.err) + 4 + 4 + 2 + len(out.UploadID)
	buff := enc.pool.Get(headerLen + bodyLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.Put32(uint32(out.MinPartSize))
	m.Put32(uint32(out.MaxCount))
	m.putString(out.UploadID)

	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write CreateMultipartUpload response")
	}
	return nil
}

func (enc *svrEncoder) encodeListResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	out := resp.out.(*listOut)
	buff := enc.pool.Get(1 << 20)
	defer enc.pool.Put(buff)
	bodyLen := 2 + len(resp.err) + 1 + 2 + len(out.nextMarker) + 4 // last 4 bytes is a placeholder for next batch length
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.putBool(out.isTruncated)
	m.putString(out.nextMarker)

	off := m.Offset()
	batchLen := 0
	m.Put32(0) // placeholder for next batch length

	var err error
	if len(out.objects) == 0 {
		_, err = conn.Write(m.Bytes())
		return err
	}

	var objLen int
	for _, obj := range out.objects {
		objLen = 2 + len(obj.key) + 2 + len(obj.sc) + 8 + 8 + 1 // key, sc, size, mtime, isDir
		if m.Left() < objLen {
			logger.Debugf("List response buffer is full, sending %d bytes", m.Offset())
			data := m.Bytes()[:m.Offset()]
			m.Seek(off)
			m.Put32(uint32(batchLen))
			if _, err := conn.Write(data); err != nil {
				return errors.Wrap(err, "failed to write List response")
			}
			m.SetBytes(buff)
			off, batchLen = 0, 0
			m.Put32(0) // reset placeholder
		}
		m.putString(obj.Key())
		m.putString(obj.StorageClass())
		m.Put64(uint64(obj.Size()))
		m.Put64(uint64(obj.Mtime().UnixNano()))
		m.putBool(obj.IsDir())
		batchLen += objLen
	}

	if batchLen > 0 {
		m.Put32(0) // end marker
		data := m.Bytes()[:m.Offset()]
		m.Seek(off)
		m.Put32(uint32(batchLen))
		if _, err := conn.Write(data); err != nil {
			return errors.Wrap(err, "failed to write List response")
		}
	}

	return nil
}

func (enc *svrEncoder) encodeHeadResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	out := resp.out.(*headOut)
	bodyLen := 2 + len(resp.err) + 2 + len(out.sc) + 8 + 1 + 8
	buff := enc.pool.Get(headerLen + bodyLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.putString(out.sc)
	m.Put64(uint64(out.size))
	m.Put64(uint64(out.mtime.UnixNano()))
	m.putBool(out.isDir)

	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write Head response")
	}
	return nil
}

func (enc *svrEncoder) encodeDelResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	rid := resp.out.(*delOut).rid
	bodyLen := 4 + len(resp.err) + len(rid)
	buff := enc.pool.Get(headerLen + bodyLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.putString(rid)
	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrapf(err, "failed to write Delete response")
	}
	return nil
}

func (enc *svrEncoder) encodePutResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	var bodyLen int
	var out *putOut
	var buff []byte
	out = resp.out.(*putOut)
	bodyLen = 6 + len(resp.err) + len(out.rid) + len(out.sc)
	buff = enc.pool.Get(headerLen + bodyLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.putString(out.rid)
	m.putString(out.sc)
	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write Put response")
	}
	return nil
}

func (enc *svrEncoder) encodeGetResp(conn net.Conn, resp *response) error {
	if resp.out == nil {
		return enc.encodeSimpleResp(conn, resp)
	}
	out := resp.out.(*getOut)
	dataLen := 0
	if out.rc != nil {
		dataLen = out.rc.Len()
	}
	bodyLen := 6 + len(resp.err) + len(out.rid) + len(out.sc) + dataLen
	buff := enc.pool.Get(headerLen + bodyLen - dataLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	m.putString(resp.err)
	m.putString(out.rid)
	m.putString(out.sc)

	var err error
	if _, err = conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write Get response")
	}
	if out.rc == nil {
		return nil
	}
	defer out.rc.Close()
	if _, err = io.Copy(conn, out.rc); err != nil {
		return errors.Wrap(err, "failed to write Get response data")
	}
	return nil
}

func (enc *svrEncoder) encodeStrResp(conn net.Conn, resp *response) error {
	name := resp.out.(string)
	bLen := 4 + len(resp.err) + len(name)
	buff := enc.pool.Get(headerLen + bLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bLen, resp.cmd)
	m.putString(resp.err)
	m.putString(name)

	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write String response")
	}
	return nil
}

func (enc *svrEncoder) encodeLimitsResp(conn net.Conn, resp *response) error {
	bLen := 2 + len(resp.err) + 26
	buff := enc.pool.Get(headerLen + bLen)
	defer enc.pool.Put(buff)
	m := newEncMsg(buff, bLen, resp.cmd)
	m.putString(resp.err)
	limits := resp.out.(*object.Limits)
	m.putBool(limits.IsSupportMultipartUpload)
	m.putBool(limits.IsSupportUploadPartCopy)
	m.Put64(uint64(limits.MinPartSize))
	m.Put64(uint64(limits.MaxPartSize))
	m.Put64(uint64(limits.MaxPartCount))
	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write Limits response")
	}
	return nil
}

// encodeSimpleResp with only error message
func (s *svrEncoder) encodeSimpleResp(conn net.Conn, resp *response) error {
	bodyLen := 2 + len(resp.err)
	if resp.err == "" {
		bodyLen = 0
	}
	buff := s.pool.Get(headerLen + bodyLen)
	defer s.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, resp.cmd)
	if resp.err != "" {
		m.putString(resp.err)
	}
	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrapf(err, "failed to write %s response", cmd2Name[resp.cmd])
	}
	return nil
}
