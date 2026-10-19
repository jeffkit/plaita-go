// Package version contains build information about Plaita.
package version

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"text/template"

	"github.com/Masterminds/semver/v3"
)

// Version contains build information about Plaita.
type Version struct {
	Version     *semver.Version `json:"version"`
	BuildBranch string          `json:"BuildBranch"`
	BuildUser   string          `json:"BuildUser"`
	BuildDate   string          `json:"buildDate"`

	goVersion
}

type goVersion struct {
	GoVersion string `json:"goVersion"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
}

var version = Version{
	Version: semver.New(0, 1, 0, "dev", ""),
	goVersion: goVersion{
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
	},
}

var versionTmpl = template.Must(template.New("version").Parse(`
{{.program}}, version {{.version}} (branch: {{.branch}})
  build user:       {{.buildUser}}
  build date:       {{.buildDate}}
  go version:       {{.goVersion}}
  platform:         {{.platform}}
`))

// Print returns version information.
func Print(program string) string {
	m := map[string]string{
		"program":   program,
		"version":   version.Version.String(),
		"branch":    version.BuildBranch,
		"buildUser": version.BuildUser,
		"buildDate": version.BuildDate,
		"goVersion": version.GoVersion,
		"platform":  version.GOOS + "/" + version.GOARCH,
	}

	var buf bytes.Buffer
	if err := versionTmpl.ExecuteTemplate(&buf, "version", m); err != nil {
		panic(err)
	}
	return strings.TrimSpace(buf.String())
}

// Info returns version, branch and revision information.
func Info() string {
	return fmt.Sprintf("(version=%s, branch=%s)", version.Version, version.BuildBranch)
}

// BuildContext returns goVersion, platform, buildUser and buildDate information.
func BuildContext() string {
	return fmt.Sprintf(
		"(go=%s, platform=%s, user=%s, date=%s)",
		version.GoVersion,
		version.GoVersion+"/"+version.GOARCH,
		version.BuildUser,
		version.BuildDate,
	)
}
