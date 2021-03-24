package processors

import (
	"fmt"
	"io"

	"github.com/indosatppi/ratchet/v3/data"
	"github.com/indosatppi/ratchet/v3/logger"
	"github.com/indosatppi/ratchet/v3/util"
)

// IoWriter wraps any io.Writer object.
// It can be used to write data out to a File, os.Stdout, or
// any other task that can be supported via io.Writer.
type IoWriter struct {
	Writer     io.Writer
	WriteFunc func(d data.JSON) (int, error)
	AddNewline bool
	Name string
}

// NewIoWriter returns a new IoWriter wrapping the given io.Writer object
func NewIoWriter(writer io.Writer) *IoWriter {
	return &IoWriter{Writer: writer, AddNewline: false}
}

func NewDynamicIoWriter(writer func(d data.JSON) (int, error)) *IoWriter {
	return &IoWriter{WriteFunc: writer, AddNewline: false}
}

// ProcessData writes the data
func (w *IoWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	var bytesWritten int
	var err error
	if w.WriteFunc != nil {
		bytesWritten, err := w.WriteFunc(d)
		util.KillPipelineIfErr(err, killChan)
		logger.Debug("IoWriter:", bytesWritten, "bytes written")
		return
	}

	if w.AddNewline {
		bytesWritten, err = fmt.Fprintln(w.Writer, string(d))
	} else {
		bytesWritten, err = w.Writer.Write(d)
	}
	util.KillPipelineIfErr(err, killChan)
	logger.Debug("IoWriter:", bytesWritten, "bytes written")
}

// Finish - see interface for documentation.
func (w *IoWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *IoWriter) String() string {
	return createProcessorName("IoWriter", w.Name)
}

func (w *IoWriter) SetName(name string) *IoWriter {
	w.Name = name
	return w
}