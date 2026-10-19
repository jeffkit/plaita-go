// Package filesystem provides file system workflow provider
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"

	"github.com/samber/lo"

	"github.com/jeffkit/plaita-go/pkg/parser"
	"github.com/jeffkit/plaita-go/pkg/providers"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

type fileSystemWorkflowProvider struct {
	mutex sync.RWMutex

	flows       map[workflow.ID]map[workflow.Version]workflow.Workflow
	flowDirPath string
}

// ProviderOptions is the options for file system workflow provider
type ProviderOptions struct {
	dirPath string
}

// ProviderOption is the option function for setting file system workflow provider config options
type ProviderOption func(o *ProviderOptions)

// WithWorkflowDirPath sets the workflow directory path
func WithWorkflowDirPath(dirPath string) ProviderOption {
	return func(o *ProviderOptions) {
		o.dirPath = dirPath
	}
}

// NewWorkflowProvider creates a file system workflow provider instance
func NewWorkflowProvider(opts ...ProviderOption) (providers.WorkflowProvider, error) {
	o := new(ProviderOptions)
	for _, opt := range opts {
		opt(o)
	}

	if len(o.dirPath) == 0 {
		return nil, errors.New("workflow directory path is required")
	}

	fp := &fileSystemWorkflowProvider{
		flowDirPath: o.dirPath,
		flows:       make(map[workflow.ID]map[workflow.Version]workflow.Workflow),
	}
	if err := fp.loadFlowsFromDirectory(); err != nil {
		return nil, err
	}

	return fp, nil
}

func (fp *fileSystemWorkflowProvider) loadFlowsFromDirectory() error {
	return filepath.WalkDir(fp.flowDirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != fp.flowDirPath {
				return filepath.SkipDir
			}
			return nil
		}

		if !lo.Contains(parser.SupportedExts, parser.FileExt(filepath.Ext(path))) {
			return nil
		}

		wf, err := workflowFromFile(path)
		if err != nil {
			return err
		}

		return fp.setWorkflow(wf)
	})
}

func workflowFromFile(path string) (*workflow.Workflow, error) {
	return parser.FromFile(path)
}

// GetWorkflow returns a flow by flowID and flowVersion.
func (fp *fileSystemWorkflowProvider) GetWorkflow(
	_ context.Context,
	id workflow.ID,
	version workflow.Version,
) (*workflow.Workflow, error) {
	fp.mutex.RLock()
	defer fp.mutex.RUnlock()

	versionWorkflows, ok := fp.flows[id]
	if !ok {
		return nil, fmt.Errorf("workflow not found for id: %s", id)
	}

	wf, ok := versionWorkflows[version]
	if !ok {
		return nil, fmt.Errorf("workflow not found for id: %s and version: %s", id, version)
	}

	return &wf, nil
}

func (fp *fileSystemWorkflowProvider) setWorkflow(wf *workflow.Workflow) error {
	if wf == nil || len(wf.ID) == 0 || len(wf.Version) == 0 {
		return errors.New("invalid workflow, must pass workflow id and version")
	}

	fp.mutex.Lock()
	defer fp.mutex.Unlock()

	versionWorkflows, ok := fp.flows[wf.ID]
	if !ok {
		versionWorkflows = make(map[workflow.Version]workflow.Workflow)
		fp.flows[wf.ID] = versionWorkflows
	}

	versionWorkflows[wf.Version] = *wf
	return nil
}
