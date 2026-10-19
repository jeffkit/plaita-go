// Command shadow-compare runs every synthetic flow sample in the plaita
// Python engine (reference) and in plaita-go (candidate), compares final
// outputs and node traces, and emits a JSON plus a human-readable report.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/pkg/shadow"
)

func main() {
	samplesDir := flag.String("samples", filepath.Join("pkg", "shadow", "testdata", "samples"), "sample directory")
	flowPath := flag.String("flow", "", "compare a single deployment-flow JSON file instead of the sample set")
	inputPath := flag.String("input", "", "input JSON object file for -flow (default {})")
	pythonCommand := flag.String("python", "python3", "python interpreter")
	pythonRepo := flag.String("python-repo", defaultPythonRepo(), "plaita Python repository root")
	timeout := flag.Duration("timeout", 120*time.Second, "per-execution timeout")
	jsonPath := flag.String("json", "", "write the structured JSON report to this file")
	mdPath := flag.String("md", "", "write the human-readable Markdown report to this file")
	filter := flag.String("filter", "", "only run samples whose name contains this substring")
	quiet := flag.Bool("quiet", false, "only print the summary line")
	flag.Parse()

	source := *samplesDir
	var samples []shadow.Sample
	var err error
	if *flowPath != "" {
		var sample shadow.Sample
		sample, err = shadow.LoadFlowSample(*flowPath, *inputPath)
		samples = []shadow.Sample{sample}
		source = *flowPath
		if *inputPath != "" {
			source += " (input: " + *inputPath + ")"
		}
	} else {
		samples, err = shadow.LoadSamples(*samplesDir)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "shadow-compare:", err)
		os.Exit(2)
	}
	if *filter != "" {
		filtered := samples[:0]
		for _, sample := range samples {
			if strings.Contains(sample.Name, *filter) {
				filtered = append(filtered, sample)
			}
		}
		samples = filtered
	}
	if len(samples) == 0 {
		fmt.Fprintln(os.Stderr, "shadow-compare: no samples selected")
		os.Exit(2)
	}

	runner := shadow.NewPythonRunner(*pythonCommand, *pythonRepo)
	runner.Timeout = *timeout
	ctx := context.Background()

	results := make([]shadow.SampleResult, 0, len(samples))
	for _, sample := range samples {
		result := shadow.RunSample(ctx, sample, runner)
		results = append(results, result)
		if !*quiet && !result.Pass {
			fmt.Fprintf(os.Stderr, "FAIL %s: %d difference(s)\n", result.Name, len(result.Diffs)+len(result.ExpectDiffs))
		}
	}

	report := shadow.NewReport(results, source, *pythonRepo, *pythonCommand, runtime.Version())
	report.PythonVersion = pythonVersion(*pythonCommand)
	if !*quiet {
		report.WriteText(os.Stdout)
	} else {
		fmt.Printf("%d/%d passed (%.1f%%)\n",
			report.Summary.Passed, report.Summary.Total, report.Summary.PassRate)
	}

	if *jsonPath != "" {
		if err := writeFile(*jsonPath, func(f *os.File) error { return report.WriteJSON(f) }); err != nil {
			fmt.Fprintln(os.Stderr, "shadow-compare: write json report:", err)
			os.Exit(2)
		}
	}
	if *mdPath != "" {
		if err := writeFile(*mdPath, func(f *os.File) error {
			report.WriteMarkdown(f)
			return nil
		}); err != nil {
			fmt.Fprintln(os.Stderr, "shadow-compare: write markdown report:", err)
			os.Exit(2)
		}
	}

	if report.Summary.Failed > 0 {
		os.Exit(1)
	}
}

func writeFile(path string, write func(*os.File) error) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return write(file)
}

func defaultPythonRepo() string {
	if repo := os.Getenv("PLAITA_PY_REPO"); repo != "" {
		return repo
	}
	return shadow.DefaultPythonRepo
}

func pythonVersion(command string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, command, "--version").CombinedOutput()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
