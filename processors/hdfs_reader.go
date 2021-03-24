package processors

import (
	"io"
	"math"
	"path"
	"path/filepath"

	"github.com/colinmarc/hdfs/v2"
	"github.com/indosatppi/ratchet/v3/data"
	"github.com/indosatppi/ratchet/v3/util"
)

type HDFSReadStreamer struct {
	client     *hdfs.Client
	path       string
	pathFunc   func(data.JSON) (string, error)
	Name       string
	BufferSize int
}

func NewHDFSReadStreamer(client *hdfs.Client, folder string, filemask string, streamBufferSize int) *HDFSReadStreamer {
	return &HDFSReadStreamer{
		client:     client,
		pathFunc:   func(d data.JSON) (string, error) { return path.Join(folder, filemask), nil },
		BufferSize: streamBufferSize,
	}
}

func NewDynamicHDFSReadStreamer(client *hdfs.Client, pathFunc func(d data.JSON) (string, error), streamBufferSize int) *HDFSReadStreamer {
	return &HDFSReadStreamer{
		client:     client,
		pathFunc:   pathFunc,
		BufferSize: streamBufferSize,
	}
}

func (r *HDFSReadStreamer) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	path, err := r.pathFunc(d)
	util.KillPipelineIfErr(err, killChan)

	paths, _, err := util.NormalizeHDFSPaths([]string{path})
	util.KillPipelineIfErr(err, killChan)

	paths, err = util.ExpandPaths(r.client, paths)
	util.KillPipelineIfErr(err, killChan)

	type Payload struct {
		FileName string `json:"file_name"`
		Content  []byte `json:"content"`
	}

	for _, f := range paths {
		r.readFile(f, killChan, func(d data.JSON) {
			outputChan <- d
		})
	}
}

func (w *HDFSReadStreamer) Finish(outputChan chan data.JSON, killChan chan error) {

}

func (r *HDFSReadStreamer) readFile(fpath string, killChan chan error, foo func(d data.JSON)) {
	fname := filepath.Base(fpath)
	fs, err := r.client.Stat(fpath)
	util.KillPipelineIfErr(err, killChan)

	if fs.IsDir() {
		return
	}
	size := fs.Size()
	buffer := make([]byte, r.BufferSize)
	totalBatch := int(math.Ceil(float64(size) / float64(cap(buffer))))
	stream := util.HDFSStream{
		Filename:   fname,
		TotalBatch: totalBatch,
		Batch:      0,
	}
	reader, err := r.client.Open(fpath)
	util.KillPipelineIfErr(err, killChan)
	defer reader.Close()

	for {
		n, err := reader.Read(buffer)
		if err != io.EOF && err != nil {
			killChan <- err
		}

		if n == 0 {
			break
		}

		stream.Payload = buffer[:n]
		stream.Batch++
		d, err := data.NewJSON(stream)
		util.KillPipelineIfErr(err, killChan)
		foo(d)
	}
}

func (w *HDFSReadStreamer) String() string {
	if w.Name != "" {
		return w.Name + "(HDFSReadStreamer)"
	}
	return "HDFSReadStreamer"
}

func (w *HDFSReadStreamer) SetName(name string) *HDFSReadStreamer {
	w.Name = name
	return w
}

func (w *HDFSReadStreamer) userDir() string {
	return path.Join("/user", w.client.User())
}
