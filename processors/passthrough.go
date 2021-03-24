package processors

import "github.com/indosatppi/ratchet/v3/data"

// Passthrough simply passes the data on to the next stage.
// We have to set a placeholder field - if we leave this as an empty struct we get some properties
// for comparison and memory addressing that are not desirable and cause comparison bugs
// (see: http://dave.cheney.net/2014/03/25/the-empty-struct)
type Passthrough struct {
	i int
	Name string
}

// NewPassthrough instantiates a new instance of Passthrough
func NewPassthrough() *Passthrough {
	return &Passthrough{}
}

// ProcessData blindly sends whatever it receives to the outputChan
func (r *Passthrough) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	outputChan <- d
}

// Finish - see interface for documentation.
func (r *Passthrough) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *Passthrough) String() string {
	return createProcessorName("Passthrough", r.Name)
}

func (r *Passthrough) SetName(name string) *Passthrough {
	r.Name = name
	return r
}