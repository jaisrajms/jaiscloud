package sdk_metastore_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	hms "github.com/slachiewicz/hms-client-go"
	hive_metastore "github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// hmsAddr returns the Dataproc Metastore Thrift serving-plane address. The
// emulator binds :9083 by default (--hms-port); JAISCLOUD_GCP_HMS_PORT overrides
// it the way the binary's own flag does.
func hmsAddr() string {
	if p := os.Getenv("JAISCLOUD_GCP_HMS_PORT"); p != "" {
		if _, err := strconv.Atoi(p); err == nil {
			return "127.0.0.1:" + p
		}
	}
	return "127.0.0.1:9083"
}

// hmsEndpoint returns the thrift:// URI the clean client dials.
func hmsEndpoint() string { return "thrift://" + hmsAddr() }

// dialHMS opens the off-the-shelf Go HMS client against the serving plane. The
// client is safe for concurrent use (it pools connections), so a single handle
// drives the concurrency cases as well as the sequential ones. It is registered
// for cleanup.
func dialHMS(t *testing.T) *hms.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := hms.New(ctx, hmsEndpoint(), hms.WithTimeout(15*time.Second))
	if err != nil {
		t.Fatalf("hms.New(%s): %v", hmsEndpoint(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// dialGeneratedHMS opens a raw binary-Thrift connection and returns the
// *generated* Hive metastore client. The clean client wraps its errors into
// package sentinels; the generated client surfaces the declared Thrift
// exception types (NoSuchObjectException, AlreadyExistsException, ...) that the
// p3 case asserts by type, and the request-struct forms (get_partitions_ps,
// get_partition_names_ps) the clean API does not expose. It is NOT safe for
// concurrent use — one connection, sequential calls — so concurrency cases use
// dialHMS instead.
func dialGeneratedHMS(t *testing.T) (*hive_metastore.ThriftHiveMetastoreClient, func()) {
	t.Helper()
	raw, err := net.DialTimeout("tcp", hmsAddr(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial HMS %s: %v", hmsAddr(), err)
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

// hmsName returns a run-scoped, Hive-legal name ([a-z0-9_]). The HMS catalog is
// single-global (F6), so every test scopes its databases/tables this way to stay
// idempotent across re-runs.
func hmsName(prefix string) string {
	return fmt.Sprintf("hms_%s_%d", prefix, time.Now().UnixNano())
}

// hmsDB builds a clean-client Database ready for CreateDatabase. LocationURI is
// set explicitly: an empty one makes the client resolve the warehouse root via
// get_config_value/get_catalog, RPCs the emulator's serving plane does not
// implement.
func hmsDB(name string) *hms.Database {
	return &hms.Database{Name: name, LocationURI: "gs://bucket/wh/" + name}
}
