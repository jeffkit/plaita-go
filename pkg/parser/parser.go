// Package parser parse the struct of the plaita workflow
package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// FileExt is a file extension type.
type FileExt string

// Supported file extensions.
const (
	JSONExt FileExt = ".json"
	YAMLExt FileExt = ".yaml"
	YMLExt  FileExt = ".yml"
)

// SupportedExts Supported file extensions.
var SupportedExts = []FileExt{YAMLExt, YMLExt, JSONExt}

// FromYAMLSource parses the given plaita Workflow YAML/YML source into the Workflow type.
func FromYAMLSource(source []byte) (*workflow.Workflow, error) {
	jsonSource, err := yaml.YAMLToJSON(source)
	if err != nil {
		return nil, err
	}
	return FromJSONSource(jsonSource)
}

// FromJSONSource parses the given plaita Workflow JSON source into the Workflow type.
func FromJSONSource(source []byte) (*workflow.Workflow, error) {
	wf := new(workflow.Workflow)
	if err := json.Unmarshal(source, wf); err != nil {
		return nil, err
	}
	if err := wf.Validate(); err != nil {
		return nil, err
	}
	return wf, nil
}

// FromFile parses the given plaita Workflow file into the Workflow type.
func FromFile(path string) (*workflow.Workflow, error) {
	if err := checkFilePath(path); err != nil {
		return nil, err
	}
	fileSource, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	if filepath.Ext(path) == string(YAMLExt) || filepath.Ext(path) == string(YMLExt) {
		return FromYAMLSource(fileSource)
	}
	return FromJSONSource(fileSource)
}

// checkFilePath verify if the file exists in the given path and if it's supported by the parser package
func checkFilePath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("file path %q must stand to a file", path)
	}
	for _, ext := range SupportedExts {
		if filepath.Ext(path) == string(ext) {
			return nil
		}
	}
	return fmt.Errorf("file extension not supported for %q", path)
}
