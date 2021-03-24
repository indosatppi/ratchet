package processors

import (
	"encoding/json"

	"gopkg.in/yaml.v2"
	"github.com/indosatppi/ratchet/v3/data"
	"github.com/indosatppi/ratchet/v3/util"
)

type YamlReader struct{
	Name string
}

func NewYamlReader() *YamlReader {
	return &YamlReader{}
}

func (r *YamlReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	buf, err := yamlToJson(d)
	util.KillPipelineIfErr(err, killChan)
	outputChan <- buf
}

func (r *YamlReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *YamlReader) String() string {
	return createProcessorName("YamlReader", r.Name)
}

func (r *YamlReader) SetName(name string) *YamlReader {
	r.Name = name
	return r
}

func yamlToJson(d data.JSON) (data.JSON, error) {
	var body interface{}
	err := yaml.Unmarshal(d, &body)
	if err != nil {
		return nil, err
	}

	body = convert(body)
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func convert(i interface{}) interface{} {
    switch x := i.(type) {
    case map[interface{}]interface{}:
        m2 := map[string]interface{}{}
        for k, v := range x {
            m2[k.(string)] = convert(v)
        }
        return m2
    case []interface{}:
        for i, v := range x {
            x[i] = convert(v)
        }
    }
    return i
}
