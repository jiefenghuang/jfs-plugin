package pkg

import (
	"bytes"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

func BenchmarkBufferPool(b *testing.B) {
	pool := newBufferPool([]int{5})
	b.Run("Get Exp", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buff := pool.Get(4)
			pool.Put(buff)
		}
	})
	b.Run("Get Spec", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buff := pool.Get(5)
			pool.Put(buff)
		}
	})
}

type fakePlugin struct {
	plugin
}

var dummyObject = make([]byte, 4<<20)
var dummyStore = make([]byte, 4<<20)

func (f *fakePlugin) get(any) (any, error) {
	return &getOut{rc: &getReader{bytes.NewReader(dummyObject)}}, nil
}

func (f *fakePlugin) put(in any) (any, error) {
	r := in.(*putIn).reader
	_, _ = io.ReadFull(r, dummyStore)
	return nil, nil
}

func (f *fakePlugin) limits(any) (any, error) {
	return &object.Limits{
		IsSupportMultipartUpload: false,
		IsSupportUploadPartCopy:  false,
		MinPartSize:              0,
		MaxPartSize:              0,
		MaxPartCount:             0,
	}, nil
}

func BenchmarkPlugin(b *testing.B) {
	b.Run("UDS", func(b *testing.B) {
		sock := path.Join(os.TempDir(), time.Now().Format("080808")+".uds")
		defer os.RemoveAll(sock)
		benchmarkConn(b, "unix", sock)
	})
	b.Run("TCP", func(b *testing.B) {
		benchmarkConn(b, "tcp", "localhost:8080")
	})
}

func benchmarkConn(b *testing.B, proto, addr string) {
	// utils.SetLogLevel(logrus.DebugLevel)
	svr := NewServer(&SvrOptions{
		Proto:    proto,
		Addr:     addr,
		BuffList: DefaultSvrCapList,
	})
	svr.setPlugin(&fakePlugin{})
	done := make(chan struct{})
	go svr.Start(done)
	defer svr.close()
	<-done

	cli, err := NewClient(&CliOptions{
		Version:  "1.3.0",
		MaxConn:  200,
		Proto:    svr.Proto,
		Addr:     svr.Addr,
		BuffList: DefaultCliCapList,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	b.SetBytes(4 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	b.Run("Put", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err = cli.Put("test", bytes.NewReader(dummyObject)); err != nil {
					panic(err)
				}
			}
		})
	})
	b.Run("Get", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err = cli.Get("test", 0, -1); err != nil {
					panic(err)
				}
			}
		})
	})
	b.SetBytes(26)
	b.Run("Limits", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _ = cli.Limits(); err != nil {
					panic(err)
				}
			}
		})
	})
}
