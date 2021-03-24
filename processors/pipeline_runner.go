package processors

import (
	"github.com/indosatppi/ratchet/v3"
	"github.com/indosatppi/ratchet/v3/data"
	"github.com/indosatppi/ratchet/v3/util"
)

type PipelineRunner struct {
	pipeline       *ratchet.Pipeline
	Name string
}

func NewPipelineRunner(pl *ratchet.Pipeline) *PipelineRunner {
	return &PipelineRunner{pipeline: pl}
}

func (s *PipelineRunner) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	s.pipeline.Run()
	select {
		case err := <- s.pipeline.Run():
			if err != nil {
				killChan <- err
			}
	}
}

// Finish - see interface for documentation.
func (s *PipelineRunner) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *PipelineRunner) String() string {
	return createProcessorName("PipelineRunner", s.Name)
}

func (s *PipelineRunner) SetName(name string) *PipelineRunner {
	s.Name = name
	return s
}