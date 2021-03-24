package processors

import (
	"io/ioutil"

	"github.com/indosatppi/ratchet/v3/data"
	"github.com/indosatppi/ratchet/v3/util"
)

// FileReader opens and reads the contents of the given filename.
type FileReader struct {
	filename string
	Name     string
}

// NewFileReader returns a new FileReader that will read the entire contents
// of the given file path and send it at once. For buffered or line-by-line
// reading try using IoReader.
func NewFileReader(filename string) *FileReader {
	return &FileReader{filename: filename}
}

// ProcessData reads a file and sends its contents to outputChan
func (r *FileReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	d, err := ioutil.ReadFile(r.filename)
	util.KillPipelineIfErr(err, killChan)
	outputChan <- d
}

// Finish - see interface for documentation.
func (r *FileReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *FileReader) String() string {
	return createProcessorName("FileReader", r.Name)
}

func (r *FileReader) SetName(name string) *FileReader {
	r.Name = name
	return r
}