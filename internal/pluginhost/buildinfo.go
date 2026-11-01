package pluginhost

import (
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// BuildInfo is what an in-process plugin must match: Go refuses to load a plugin
// built with another toolchain, other flags, or other source for a shared package.
type BuildInfo struct {
	Go        string `json:"go"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CGO       bool   `json:"cgo"`
	Tags      string `json:"tags"`
	Trimpath  bool   `json:"trimpath"`
	SDK       string `json:"sdk"`
	InProcess bool   `json:"inProcess"`
}

// Build describes how this binary was built.
func Build() BuildInfo {
	b := BuildInfo{Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, SDK: sdk.Version, InProcess: InProcessSupported()}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "CGO_ENABLED":
			b.CGO = s.Value == "1"
		case "-tags":
			b.Tags = s.Value
		case "-trimpath":
			b.Trimpath = s.Value == "true"
		}
	}
	return b
}

func (b BuildInfo) String() string {
	tags := b.Tags
	if tags == "" {
		tags = "none"
	}
	return fmt.Sprintf("built with %s %s/%s, cgo=%t, tags=%s, trimpath=%t, sdk=%s (require github.com/jaisrajms/jaiscloud/sdk %s)",
		b.Go, b.OS, b.Arch, b.CGO, tags, b.Trimpath, b.SDK, b.SDK)
}

// Line is the one-line summary the version command prints.
func (b BuildInfo) Line() string {
	if !b.InProcess {
		return "in-process plugins: not supported by this build (needs CGO_ENABLED=1 on linux or darwin; see `make build-plugins`)"
	}
	return "in-process plugins: supported, " + b.String()
}
