//go:build gcp_persistence

package hms_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	hive_metastore "github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// gcpBin returns the path to the jaiscloud-gcp binary. Tests run with the
// working directory set to tests/persistent_mode/gcp/hms/, so the project-root
// binary is four levels up. JAISCLOUD_GCP_BIN overrides.
func gcpBin() string {
	if b := os.Getenv("JAISCLOUD_GCP_BIN"); b != "" {
		return b
	}
	const rel = "../../../../jaiscloud-gcp"
	if _, err := os.Stat(rel); err == nil {
		return rel
	}
	return "jaiscloud-gcp"
}

// persistPort returns the control-plane (HTTP) port for the managed server.
func persistPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8099
}

// hmsPort returns the Thrift serving-plane port the managed server binds. Its
// non-default default (9084) keeps the suite from colliding with a developer's
// emulator on :9083 or another persistence package.
func hmsPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_HMS_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 9084
}

type testWriter struct {
	t      *testing.T
	prefix string
	buf    string
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.buf += string(p)
	for {
		idx := strings.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.t.Log(w.prefix + w.buf[:idx])
		w.buf = w.buf[idx+1:]
	}
	return len(p), nil
}

// startGCPProcess starts a managed jaiscloud-gcp subprocess bound to the given
// control-plane and Thrift serving-plane ports. Callers kill it when done.
func startGCPProcess(t *testing.T, port, hport int, dsn, blobDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(gcpBin(),
		"start",
		"--port", strconv.Itoa(port),
		"--grpc-port", strconv.Itoa(port+1),
		"--hms-port", strconv.Itoa(hport),
		"--dsn", dsn,
		"--blob-dir", blobDir,
		"--log-level", "warn",
	)
	cmd.Stdout = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d]: ", port)}
	cmd.Stderr = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d] ERR: ", port)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start jaiscloud-gcp on port %d: %v", port, err)
	}
	return cmd
}

// waitForHealth polls /_jaiscloud/health until the server responds 200 or times out.
func waitForHealth(t *testing.T, host string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(host + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", host)
}

// stopProcess kills the managed server and waits for the control-plane port to
// be released before returning.
func stopProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	_ = cmd.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", persistPort()))
		if err != nil {
			return
		}
		c.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("jaiscloud-gcp did not release the port after kill")
}

// dialGenerated opens a raw binary-Thrift connection to the serving plane and
// returns the *generated* Hive metastore client. The clean hms API does not
// surface get_table_meta or alter_table_with_cascade, so the Hive-3.x paths are
// driven through the generated package (the same one the clean client wraps).
// The returned close func tears the connection down.
func dialGenerated(t *testing.T, addr string) (*hive_metastore.ThriftHiveMetastoreClient, func()) {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial HMS %s: %v", addr, err)
	}
	tcfg := &thrift.TConfiguration{}
	trans := thrift.NewTSocketFromConnConf(raw, tcfg)
	buf := thrift.NewTBufferedTransport(trans, 8192)
	proto := thrift.NewTBinaryProtocolConf(buf, tcfg)
	client := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(proto, proto))
	return client, func() {
		_ = buf.Close()
		_ = raw.Close()
	}
}
