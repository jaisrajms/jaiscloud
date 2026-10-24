//go:build release

package release

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// gcpBinary is the binary name GoReleaser packs for the GCP cloud (the windows
// archive carries jaiscloud-gcp.exe).
const gcpBinary = "jaiscloud-gcp"

// The release contract asserted below: every OS/arch pair GoReleaser builds for
// jaiscloud-gcp, and the linux package formats nfpm emits.
var (
	gcpArchiveRE = regexp.MustCompile(`^jaiscloud-gcp_([^_]+)_(linux|darwin|windows)_(amd64|arm64)\.(tar\.gz|zip)$`)
	gcpPackageRE = regexp.MustCompile(`^jaiscloud-gcp_([^_]+)_linux_(amd64|arm64)\.(deb|rpm)$`)
)

// archiveInfo is one release archive found under dist/.
type archiveInfo struct {
	path    string
	version string
	goos    string
	goarch  string
	format  string
	modTime time.Time
}

func (a archiveInfo) binaryName() string {
	if a.goos == "windows" {
		return gcpBinary + ".exe"
	}
	return gcpBinary
}

// distRoot returns the GoReleaser output directory (override with
// JAISCLOUD_DIST). The test's working directory is tests/release, so the repo
// default is two levels up.
func distRoot(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("JAISCLOUD_DIST")
	if dir == "" {
		dir = filepath.Join("..", "..", "dist")
	}
	abs, err := filepath.Abs(dir)
	require.NoError(t, err)
	return abs
}

// newestArchives indexes dist/ by "<goos>/<goarch>", keeping the most recently
// modified archive per target so a stale tree with an older version is not
// mistaken for the current one (equal mtimes break towards the newer version).
func newestArchives(t *testing.T, dir string) map[string]archiveInfo {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoErrorf(t, err, "no dist/ at %s — run `goreleaser release --snapshot --clean --skip=publish,docker` first", dir)

	out := make(map[string]archiveInfo)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := gcpArchiveRE.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		info, err := entry.Info()
		require.NoError(t, err)
		key := match[2] + "/" + match[3]
		if current, ok := out[key]; ok {
			older := info.ModTime().Before(current.modTime)
			sameTimeOlderVersion := info.ModTime().Equal(current.modTime) && match[1] < current.version
			if older || sameTimeOlderVersion {
				continue
			}
		}
		out[key] = archiveInfo{
			path:    filepath.Join(dir, entry.Name()),
			version: match[1],
			goos:    match[2],
			goarch:  match[3],
			format:  match[4],
			modTime: info.ModTime(),
		}
	}
	return out
}

// archiveEntries lists the member names (with modes) of a tar.gz or zip archive.
func archiveEntries(t *testing.T, a archiveInfo) map[string]os.FileMode {
	t.Helper()
	f, err := os.Open(a.path)
	require.NoError(t, err)
	defer f.Close()

	entries := make(map[string]os.FileMode)
	switch a.format {
	case "zip":
		stat, err := f.Stat()
		require.NoError(t, err)
		zr, err := zip.NewReader(f, stat.Size())
		require.NoError(t, err)
		for _, file := range zr.File {
			entries[file.Name] = file.Mode().Perm()
		}
	default:
		gz, err := gzip.NewReader(f)
		require.NoError(t, err)
		defer gz.Close()
		tr := tar.NewReader(gz)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			require.NoErrorf(t, err, "read %s", a.path)
			entries[hdr.Name] = os.FileMode(hdr.Mode).Perm()
		}
	}
	return entries
}

// extractBinary writes the archive's binary to a temp dir and returns its path.
// It refuses to return a non-executable binary: an archive that stores the
// binary 0644 extracts into "permission denied" for users too.
func extractBinary(t *testing.T, a archiveInfo) string {
	t.Helper()
	dest := filepath.Join(t.TempDir(), a.binaryName())
	mode := os.FileMode(0o755)
	for name, stored := range archiveEntries(t, a) {
		if name == a.binaryName() {
			mode = stored
		}
	}
	if a.goos != "windows" {
		require.NotZerof(t, mode&0o111, "%s stores %s without execute permission (%04o)", a.path, a.binaryName(), mode)
	}

	f, err := os.Open(a.path)
	require.NoError(t, err)
	defer f.Close()

	copyMember := func(r io.Reader) {
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		require.NoError(t, err)
		defer out.Close()
		_, err = io.Copy(out, r)
		require.NoError(t, err)
	}

	found := false
	switch a.format {
	case "zip":
		stat, err := f.Stat()
		require.NoError(t, err)
		zr, err := zip.NewReader(f, stat.Size())
		require.NoError(t, err)
		for _, file := range zr.File {
			if file.Name != a.binaryName() {
				continue
			}
			rc, err := file.Open()
			require.NoError(t, err)
			defer rc.Close()
			copyMember(rc)
			found = true
		}
	default:
		gz, err := gzip.NewReader(f)
		require.NoError(t, err)
		defer gz.Close()
		tr := tar.NewReader(gz)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			if hdr.Name == a.binaryName() {
				copyMember(tr)
				found = true
				break
			}
		}
	}
	require.Truef(t, found, "archive %s has no %s", a.path, a.binaryName())
	require.NoError(t, os.Chmod(dest, 0o755))
	return dest
}

// sortedKeys keeps assertions deterministic (map order is random).
func sortedKeys(m map[string]archiveInfo) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// archiveVersions returns the distinct versions present in the archive set.
func archiveVersions(archives map[string]archiveInfo) []string {
	seen := make(map[string]struct{})
	for _, a := range archives {
		seen[a.version] = struct{}{}
	}
	versions := make([]string, 0, len(seen))
	for v := range seen {
		versions = append(versions, v)
	}
	sort.Strings(versions)
	return versions
}

// hostNativeArchive returns the archive built for the host, if any.
func hostNativeArchive(archives map[string]archiveInfo) (archiveInfo, bool) {
	for _, a := range archives {
		if a.goos == runtime.GOOS && a.goarch == runtime.GOARCH {
			return a, true
		}
	}
	return archiveInfo{}, false
}

// TestGCPArchives asserts every built target ships one archive with the binary
// and the licence/readme files, all stamped with one version.
func TestGCPArchives(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: release artifacts need a goreleaser snapshot")
	}
	dir := distRoot(t)
	archives := newestArchives(t, dir)
	require.NotEmptyf(t, archives, "no jaiscloud-gcp archives in %s", dir)

	// The release builds linux/darwin/windows × amd64/arm64; a missing target
	// means a goos/goarch entry was dropped from .goreleaser.yaml.
	for _, target := range []string{
		"linux/amd64", "linux/arm64",
		"darwin/amd64", "darwin/arm64",
		"windows/amd64", "windows/arm64",
	} {
		require.Containsf(t, archives, target, "no jaiscloud-gcp archive for %s", target)
	}

	for _, target := range sortedKeys(archives) {
		a := archives[target]
		entries := archiveEntries(t, a)
		// GoReleaser adds LICENSE*/README*/CHANGELOG* from the repo root next to
		// the binary; anything else (a stray file, a second binary) is a bug.
		required := []string{a.binaryName()}
		for _, name := range []string{"LICENSE", "README-GCP.md", "README.md"} {
			required = append(required, name)
		}
		for _, name := range required {
			require.Containsf(t, entries, name, "%s (%s) should contain %s", a.path, target, name)
		}
		for name := range entries {
			if name == a.binaryName() {
				continue
			}
			require.Truef(t, isReleaseDoc(name), "%s (%s) contains an unexpected member %q", a.path, target, name)
		}
	}
	require.Lenf(t, archiveVersions(archives), 1,
		"dist/ mixes archive versions %v — re-run `goreleaser release --snapshot --clean`", archiveVersions(archives))
}

// isReleaseDoc reports whether name is one of the repo-root documents GoReleaser
// is allowed to pack next to the binary.
func isReleaseDoc(name string) bool {
	for _, prefix := range []string{"LICENSE", "README", "CHANGELOG"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// TestGCPArchiveVersion runs `jaiscloud-gcp version` from the host-native
// archive and asserts it reports the version in the archive name. Dropping
// `-X main.version={{.Version}}` from the build's ldflags turns this red.
func TestGCPArchiveVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: release artifacts need a goreleaser snapshot")
	}
	archives := newestArchives(t, distRoot(t))
	a, ok := hostNativeArchive(archives)
	require.Truef(t, ok, "no host-native archive in dist/ (host %s/%s)", runtime.GOOS, runtime.GOARCH)

	binaryPath := extractBinary(t, a)
	out, err := exec.Command(binaryPath, "version").CombinedOutput()
	require.NoErrorf(t, err, "%s version: %s", binaryPath, out)

	// The GCP build prints "jaiscloud-gcp <version>"; compare the first two
	// fields so an added commit/date suffix (as the aws build has) does not
	// masquerade as a release regression.
	fields := strings.Fields(string(out))
	require.GreaterOrEqualf(t, len(fields), 2, "%s version printed %q", binaryPath, out)
	require.Equalf(t, gcpBinary, fields[0], "%s version printed %q", binaryPath, out)
	require.Equalf(t, a.version, fields[1],
		"%s should report the archive version (check the -X main.version ldflag): %q", filepath.Base(a.path), out)
	require.NotEqual(t, "dev", fields[1], "%s was built without the version ldflag", filepath.Base(a.path))
}

// TestGCPArchiveServesRESTAndConsole starts the host-native archive binary and
// asserts it answers /_jaiscloud/health and serves the embedded console. A
// build without `-tags ui` compiles fine, passes every other assertion here, and
// still ships no console — only this test notices.
func TestGCPArchiveServesRESTAndConsole(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: release artifacts need a goreleaser snapshot")
	}
	archives := newestArchives(t, distRoot(t))
	a, ok := hostNativeArchive(archives)
	require.Truef(t, ok, "no host-native archive in dist/ (host %s/%s)", runtime.GOOS, runtime.GOARCH)

	binaryPath := extractBinary(t, a)
	restPort, grpcPort, hmsPort, uiPort := freePort(t), freePort(t), freePort(t), freePort(t)

	var logs bytes.Buffer
	cmd := exec.Command(binaryPath, "start", "--ephemeral",
		"--port", fmt.Sprint(restPort),
		"--grpc-port", fmt.Sprint(grpcPort),
		"--hms-port", fmt.Sprint(hmsPort),
		"--ui", "--ui-port", fmt.Sprint(uiPort),
	)
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	require.NoErrorf(t, cmd.Start(), "start %s", binaryPath)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	status, err := waitForStatus(fmt.Sprintf("http://127.0.0.1:%d/_jaiscloud/health", restPort))
	require.Equalf(t, http.StatusOK, status, "%s should serve /_jaiscloud/health (last error: %v)\n%s", filepath.Base(a.path), err, logs.String())

	status, err = waitForStatus(fmt.Sprintf("http://127.0.0.1:%d/ui/", uiPort))
	require.Equalf(t, http.StatusOK, status, "%s should serve /ui/ — was it built with -tags ui and internal/ui/dist?\n%s", filepath.Base(a.path), logs.String())
}

// TestGCPPackages asserts nfpm produced deb and rpm packages for both linux
// architectures, stamped with the same version as the archives.
func TestGCPPackages(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: release artifacts need a goreleaser snapshot")
	}
	dir := distRoot(t)
	archives := newestArchives(t, dir)
	versions := archiveVersions(archives)
	require.Lenf(t, versions, 1, "dist/ mixes archive versions %v — re-run `goreleaser release --snapshot --clean`", versions)
	version := versions[0]

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	type pkg struct {
		version string
		size    int64
	}
	found := make(map[string]pkg)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := gcpPackageRE.FindStringSubmatch(entry.Name())
		if match == nil || match[1] != version {
			continue // stale package from an earlier snapshot
		}
		info, err := entry.Info()
		require.NoError(t, err)
		found[match[3]+" "+match[2]] = pkg{version: match[1], size: info.Size()}
	}
	for _, arch := range []string{"amd64", "arm64"} {
		for _, format := range []string{"deb", "rpm"} {
			key := format + " " + arch
			got, ok := found[key]
			require.Truef(t, ok, "no %s package for linux/%s at version %s in %s", format, arch, version, dir)
			require.Positivef(t, got.size, "%s package for linux/%s is empty", format, arch)
			require.Equalf(t, version, got.version, "%s package for linux/%s has a different version than the archives", format, arch)
		}
	}
}

// TestGCPImageSmoke checks the container image serves the REST health endpoint
// and the embedded console. It runs only against a local image: JAISCLOUD_GCP_IMAGE,
// or a Docker Image artifact in dist/artifacts.json (a snapshot built without
// --skip=docker). It never pulls: a published reference from artifacts.json is
// skipped when it is not in the local image store.
func TestGCPImageSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: image smoke")
	}
	dir := distRoot(t)
	image := os.Getenv("JAISCLOUD_GCP_IMAGE")
	if image == "" {
		wantVersion := ""
		if versions := archiveVersions(newestArchives(t, dir)); len(versions) == 1 {
			wantVersion = versions[0]
		}
		image = gcpImageFromArtifacts(dir, wantVersion)
	}
	if image == "" {
		t.Skip("no jaiscloud-gcp image (set JAISCLOUD_GCP_IMAGE or build a snapshot without --skip=docker)")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker daemon not reachable: %s", out)
	}
	if out, err := exec.Command("docker", "image", "inspect", image).CombinedOutput(); err != nil {
		t.Skipf("image %s is not in the local image store (not pulling): %s", image, out)
	}

	out := runCommand(t, "docker", "run", "--rm", image, "version")
	require.Containsf(t, out, gcpBinary+" ", "image %s should answer `version`: %s", image, out)
	require.NotContainsf(t, out, gcpBinary+" dev", "image %s was built without the version ldflag", image)

	container := fmt.Sprintf("jaiscloud-gcp-release-smoke-%d", os.Getpid())
	_ = exec.Command("docker", "rm", "-f", container).Run()
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })

	runCommand(t, "docker", "run", "-d", "--name", container,
		"-p", "127.0.0.1::8080", "-p", "127.0.0.1::4567", image, "start", "--ui")

	// The image EXPOSEs REST 8080; the console listener (4567) is only started
	// with --ui, so it is published explicitly above.
	restAddr := publishedAddr(t, container, "8080/tcp")
	uiAddr := publishedAddr(t, container, "4567/tcp")

	status, err := waitForStatus("http://" + restAddr + "/_jaiscloud/health")
	require.Equalf(t, http.StatusOK, status, "%s should serve /_jaiscloud/health (last error: %v)", image, err)
	status, err = waitForStatus("http://" + uiAddr + "/ui/")
	require.Equalf(t, http.StatusOK, status, "%s should serve /ui/ (built with -tags ui; last error: %v)", image, err)
}

// gcpImageFromArtifacts finds a jaiscloud-gcp image reference GoReleaser
// recorded in dist/artifacts.json, preferring the archive version's tag.
func gcpImageFromArtifacts(dir, wantVersion string) string {
	data, err := os.ReadFile(filepath.Join(dir, "artifacts.json"))
	if err != nil {
		return ""
	}
	var artifacts []map[string]any
	if err := json.Unmarshal(data, &artifacts); err != nil {
		return ""
	}
	wantTag := ""
	if wantVersion != "" {
		wantTag = ":" + wantVersion
	}
	fallback := ""
	for _, artifact := range artifacts {
		kind, _ := artifact["type"].(string)
		if kind != "Docker Image" {
			continue
		}
		for _, field := range []string{"name", "path"} {
			ref, _ := artifact[field].(string)
			if !strings.Contains(ref, gcpBinary+":") {
				continue
			}
			if wantTag != "" && strings.HasSuffix(ref, wantTag) {
				return ref
			}
			if fallback == "" {
				fallback = ref
			}
		}
	}
	return fallback
}

func runCommand(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	require.NoErrorf(t, err, "%s %s failed:\n%s", name, strings.Join(args, " "), out)
	return string(out)
}

// publishedAddr returns the host address of a published container port.
func publishedAddr(t *testing.T, container, port string) string {
	t.Helper()
	out := runCommand(t, "docker", "port", container, port)
	addr := strings.TrimSpace(strings.Split(out, "\n")[0])
	require.Containsf(t, addr, ":", "docker port %s %s: %q", container, port, out)
	return addr
}

// freePort reserves an ephemeral port and releases it; the binary binds it
// immediately afterwards.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// waitForStatus polls url until it answers 200 or the deadline expires, then
// returns the last status code and the last transport error (for the failure
// message: "connection refused" and "404" need different fixes).
func waitForStatus(url string) (int, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	status, lastErr := 0, error(nil)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		status = resp.StatusCode
		lastErr = fmt.Errorf("HTTP %d", status)
		_ = resp.Body.Close()
		if status == http.StatusOK {
			return status, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return status, lastErr
}
