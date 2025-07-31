package server

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jiefenghuang/jfs-plugin/pkg/msg"
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
	proto, addr := msg.SplitAddr(opt.URL)
	if proto == "" || addr == "" {
		return errors.Errorf("invalid address format %s, expected 'tcp://<addr>' or 'unix://<path>'", opt.URL)
	}
	opt.proto, opt.addr = proto, addr
	if err := msg.CheckProto(opt.proto); err != nil {
		return err
	}
	if opt.BuffList == nil {
		opt.BuffList = msg.DefaultSvrCapList
	}
	return nil
}

type server struct {
	*SvrOptions
	running  bool
	listener net.Listener
	enc      *svrEncoder
	dec      *svrDecoder
	pool     msg.BytesPool
	handlers map[byte]func(any) (any, error)
}

func NewServer(opt *SvrOptions) (*server, error) {
	if err := opt.Check(); err != nil {
		return nil, err
	}
	pool := msg.NewBytesPool(opt.BuffList)
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
		msg.CmdPut:            pl.put,
		msg.CmdGet:            pl.get,
		msg.CmdStr:            pl.str,
		msg.CmdLimits:         pl.limits,
		msg.CmdInit:           pl.init,
		msg.CmdCreate:         pl.create,
		msg.CmdDel:            pl.delete,
		msg.CmdCopy:           pl.copy,
		msg.CmdHead:           pl.head,
		msg.CmdSetSC:          pl.setStorageClass,
		msg.CmdList:           pl.list,
		msg.CmdCreateMPU:      pl.createMultipartUpload,
		msg.CmdUploadPart:     pl.uploadPart,
		msg.CmdUploadPartCopy: pl.uploadPartCopy,
		msg.CmdCompleteMPU:    pl.completeMultipartUpload,
		msg.CmdListMPU:        pl.listMultipartUploads,
		msg.CmdAbortMPU:       pl.abortMultipartUpload,
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
			logger.Errorf("failed to encode response for command %s: %v", msg.Cmd2Name[resp.cmd], err)
			break
		}
		req.clear()
	}
}

func (s *server) auth(conn net.Conn) error {
	defer conn.SetDeadline(time.Time{})

	var err error
	buff := s.pool.Get(msg.HeaderLen)
	defer s.pool.Put(buff)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err = io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read CmdAuth header")
	}
	m := msg.NewMsg(buff)
	bLen, cmd := m.GetHeader()
	if cmd != msg.CmdAuth {
		return errors.Wrap(err, "the first request must be CmdAuth")
	}

	buff2 := s.pool.Get(int(bLen))
	defer s.pool.Put(buff2)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, buff2); err != nil {
		return errors.Wrap(err, "failed to read init payload")
	}
	m.SetBytes(buff2)
	ver := version.Parse(m.GetString())
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
	return s.enc.encoders[msg.CmdAuth](conn, &response{
		cmd: msg.CmdAuth,
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
	r.cmd = msg.CmdUnknown
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
	pool     msg.BytesPool
}

func newSvrDecoder(buffPool msg.BytesPool) *svrDecoder {
	dec := &svrDecoder{
		pool: buffPool,
	}
	dec.decoders = map[byte]func(net.Conn, *request) error{
		msg.CmdInit:           dec.decodeInitReq,
		msg.CmdPut:            dec.decodePutReq,
		msg.CmdGet:            dec.decodeGetReq,
		msg.CmdStr:            dec.decodeEmptyReq,
		msg.CmdLimits:         dec.decodeEmptyReq,
		msg.CmdCreate:         dec.decodeEmptyReq,
		msg.CmdDel:            dec.decodeDeleteReq,
		msg.CmdCopy:           dec.decodeCopyReq,
		msg.CmdHead:           dec.decodeHeadReq,
		msg.CmdSetSC:          dec.decodeSetSCReq,
		msg.CmdList:           dec.decodeListReq,
		msg.CmdCreateMPU:      dec.decodeCreateMPUReq,
		msg.CmdUploadPart:     dec.decodeUploadPartReq,
		msg.CmdUploadPartCopy: dec.decodeUploadPartCopyReq,
		msg.CmdAbortMPU:       dec.decodeAbortMPUReq,
		msg.CmdCompleteMPU:    dec.decodeCompleteMPUReq,
		msg.CmdListMPU:        dec.decodeListMPUReq,
	}
	return dec
}

func (dec *svrDecoder) decode(conn net.Conn, req *request) error {
	var err error
	buff := dec.pool.Get(msg.HeaderLen)
	if _, err = io.ReadFull(conn, buff); err != nil {
		dec.pool.Put(buff)
		return errors.Wrap(err, "failed to read header")
	}
	req.bodyLen = binary.BigEndian.Uint32(buff[:4])
	req.cmd = buff[4]
	dec.pool.Put(buff)

	if req.cmd <= msg.CmdUnknown || req.cmd >= msg.CmdMax {
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

	m := msg.NewMsg(buff)
	req.in = &initIn{
		endpoint:  m.GetString(),
		accesskey: m.GetString(),
		secretkey: m.GetString(),
		token:     m.GetString(),
	}
	return nil
}

func (dec *svrDecoder) decodeListMPUReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read ListMultipartUploads payload")
	}
	m := msg.NewMsg(buff)
	req.in = m.GetString()
	return nil
}

func (dec *svrDecoder) decodeCompleteMPUReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read CompleteMultipartUpload payload")
	}
	m := msg.NewMsg(buff)
	in := &completeMPUIn{
		key:      m.GetString(),
		uploadID: m.GetString(),
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
				ETag: m.GetString(),
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
	m := msg.NewMsg(buff)
	req.in = &abortMPUIn{
		key:      m.GetString(),
		uploadID: m.GetString(),
	}
	return nil
}

func (dec *svrDecoder) decodeUploadPartCopyReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read UploadPartCopy payload")
	}

	m := msg.NewMsg(buff)
	req.in = &uploadPartCopyIn{
		dstKey:   m.GetString(),
		uploadID: m.GetString(),
		num:      int(m.Get32()),
		srcKey:   m.GetString(),
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

	m := msg.NewMsg(buff)
	req.in = &uloadPartIn{
		key:      m.GetString(),
		uploadID: m.GetString(),
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
	m := msg.NewMsg(buff)
	req.in = m.GetString()
	return nil
}

func (dec *svrDecoder) decodePutReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Put payload")
	}

	m := msg.NewMsg(buff)
	req.in = &putIn{
		key:    m.GetString(),
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
	m := msg.NewMsg(buff)
	req.in = m.GetString()
	return nil
}

func (dec *svrDecoder) decodeCopyReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Copy payload")
	}
	m := msg.NewMsg(buff)
	req.in = &copyIn{dst: m.GetString(), src: m.GetString()}
	return nil
}

func (dec *svrDecoder) decodeListReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read List payload")
	}
	m := msg.NewMsg(buff)
	req.in = &listIn{
		prefix:     m.GetString(),
		startAfter: m.GetString(),
		token:      m.GetString(),
		delimiter:  m.GetString(),
		limit:      int64(m.Get64()),
		followLink: m.GetBool(),
	}
	return nil
}

func (dec *svrDecoder) decodeSetSCReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read SetStorageClass payload")
	}
	m := msg.NewMsg(buff)
	req.in = m.GetString()
	return nil
}

func (dec *svrDecoder) decodeDeleteReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Get payload")
	}
	m := msg.NewMsg(buff)
	req.in = m.GetString()
	return nil
}

func (dec *svrDecoder) decodeGetReq(conn net.Conn, req *request) error {
	buff := dec.pool.Get(int(req.bodyLen))
	defer dec.pool.Put(buff)
	if _, err := io.ReadFull(conn, buff); err != nil {
		return errors.Wrap(err, "failed to read Get payload")
	}
	m := msg.NewMsg(buff)
	req.in = &getIn{
		key:    m.GetString(),
		offset: int64(m.Get64()),
		limit:  int64(m.Get64()),
	}
	return nil
}

func (dec *svrDecoder) decodeEmptyReq(conn net.Conn, req *request) error { return nil }

type svrEncoder struct {
	pool     msg.BytesPool
	encoders map[byte]func(net.Conn, *response) error
}

func newSvrEncoder(buffPool msg.BytesPool) *svrEncoder {
	enc := &svrEncoder{pool: buffPool}
	enc.encoders = map[byte]func(net.Conn, *response) error{
		msg.CmdAuth:           enc.encodeSimpleResp,
		msg.CmdPut:            enc.encodePutResp,
		msg.CmdGet:            enc.encodeGetResp,
		msg.CmdStr:            enc.encodeStrResp,
		msg.CmdLimits:         enc.encodeLimitsResp,
		msg.CmdInit:           enc.encodeSimpleResp,
		msg.CmdCreate:         enc.encodeSimpleResp,
		msg.CmdDel:            enc.encodeDelResp,
		msg.CmdCopy:           enc.encodeSimpleResp,
		msg.CmdHead:           enc.encodeHeadResp,
		msg.CmdSetSC:          enc.encodeSimpleResp,
		msg.CmdList:           enc.encodeListResp,
		msg.CmdCreateMPU:      enc.encodeCreateMPUResp,
		msg.CmdUploadPart:     enc.encodeUploadPartResp,
		msg.CmdUploadPartCopy: enc.encodeUploadPartResp,
		msg.CmdAbortMPU:       enc.encodeSimpleResp,
		msg.CmdCompleteMPU:    enc.encodeSimpleResp,
		msg.CmdListMPU:        enc.encodeListMPUResp,
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
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.PutString(out.nextMarker)

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
		m.PutString(part.Key)
		m.PutString(part.UploadID)
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
	buff := enc.pool.Get(msg.HeaderLen + bodyLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.Put32(uint32(out.Num))
	m.Put32(uint32(out.Size))
	m.PutString(out.ETag)
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
	buff := enc.pool.Get(msg.HeaderLen + bodyLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.Put32(uint32(out.MinPartSize))
	m.Put32(uint32(out.MaxCount))
	m.PutString(out.UploadID)

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
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.PutBool(out.isTruncated)
	m.PutString(out.nextMarker)

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
		m.PutString(obj.Key())
		m.PutString(obj.StorageClass())
		m.Put64(uint64(obj.Size()))
		m.Put64(uint64(obj.Mtime().UnixNano()))
		m.PutBool(obj.IsDir())
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
	buff := enc.pool.Get(msg.HeaderLen + bodyLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.PutString(out.sc)
	m.Put64(uint64(out.size))
	m.Put64(uint64(out.mtime.UnixNano()))
	m.PutBool(out.isDir)

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
	buff := enc.pool.Get(msg.HeaderLen + bodyLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.PutString(rid)
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
	buff = enc.pool.Get(msg.HeaderLen + bodyLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.PutString(out.rid)
	m.PutString(out.sc)
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
	buff := enc.pool.Get(msg.HeaderLen + bodyLen - dataLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	m.PutString(resp.err)
	m.PutString(out.rid)
	m.PutString(out.sc)

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
	buff := enc.pool.Get(msg.HeaderLen + bLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bLen, resp.cmd)
	m.PutString(resp.err)
	m.PutString(name)

	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write String response")
	}
	return nil
}

func (enc *svrEncoder) encodeLimitsResp(conn net.Conn, resp *response) error {
	bLen := 2 + len(resp.err) + 26
	buff := enc.pool.Get(msg.HeaderLen + bLen)
	defer enc.pool.Put(buff)
	m := msg.NewEncMsg(buff, bLen, resp.cmd)
	m.PutString(resp.err)
	limits := resp.out.(*object.Limits)
	m.PutBool(limits.IsSupportMultipartUpload)
	m.PutBool(limits.IsSupportUploadPartCopy)
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
	buff := s.pool.Get(msg.HeaderLen + bodyLen)
	defer s.pool.Put(buff)
	m := msg.NewEncMsg(buff, bodyLen, resp.cmd)
	if resp.err != "" {
		m.PutString(resp.err)
	}
	if _, err := conn.Write(m.Bytes()); err != nil {
		return errors.Wrapf(err, "failed to write %s response", msg.Cmd2Name[resp.cmd])
	}
	return nil
}
