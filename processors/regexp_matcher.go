package processors

import (
	"regexp"

	"github.com/indosatppi/ratchet/v3/data"
	"github.com/indosatppi/ratchet/v3/logger"
	"github.com/indosatppi/ratchet/v3/util"
)

// RegexpMatcher checks if incoming data matches the given Regexp, and sends
// it on to the next stage only if it matches.
// It is using regexp.Match under the covers: https://golang.org/pkg/regexp/#Match
type RegexpMatcher struct {
	pattern string
	// Set to true to log each match attempt (logger must be in debug mode).
	DebugLog bool
	Name string
}

// NewRegexpMatcher returns a new RegexpMatcher initialized
// with the given pattern to match.
func NewRegexpMatcher(pattern string) *RegexpMatcher {
	return &RegexpMatcher{pattern: pattern}
}

// ProcessData sends the data it receives to the outputChan only if it matches the supplied regex
func (r *RegexpMatcher) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	matches, err := regexp.Match(r.pattern, d)
	util.KillPipelineIfErr(err, killChan)
	if r.DebugLog {
		logger.Debug("RegexpMatcher: checking if", string(d), "matches pattern", r.pattern, ". MATCH=", matches)
	}
	if matches {
		outputChan <- d
	}
}

// Finish - see interface for documentation.
func (r *RegexpMatcher) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *RegexpMatcher) String() string {
	return createProcessorName("RegexpMatcher", r.Name)
}

func (r *RegexpMatcher) SetName(name string) *RegexpMatcher {
	r.Name = name
	return r
}
