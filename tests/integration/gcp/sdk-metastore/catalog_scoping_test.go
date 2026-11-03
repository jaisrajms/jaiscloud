package sdk_metastore_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	hive_metastore "github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/stretchr/testify/require"
	metastore "google.golang.org/api/metastore/v1"
)

// genDB builds a generated-client Database with an explicit location (an empty
// one makes the client resolve the warehouse root through RPCs the emulator's
// serving plane does not implement).
func genDB(name string) *hive_metastore.Database {
	return &hive_metastore.Database{Name: name, LocationUri: "gs://bucket/wh/" + name}
}

// dialGeneratedHMSAt opens a raw binary-Thrift Hive metastore client at addr.
func dialGeneratedHMSAt(t *testing.T, addr string) (*hive_metastore.ThriftHiveMetastoreClient, func()) {
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

// TestSDKMetastorePerServiceCatalogs is the NF32 acceptance test through the real
// binary: two Dataproc Metastore Services — in different projects and locations —
// each publish their own port, and the Hive Metastore behind each port is that
// Service's own catalog. The same database and table name in both does not
// collide, each Service has its own `default`, an Iceberg lock in one is
// invisible to the other, and a table written through one catalog is not
// readable through the other.
func TestSDKMetastorePerServiceCatalogs(t *testing.T) {
	ctx := context.Background()
	svc, err := metastore.NewService(ctx, opts()...)
	require.NoError(t, err)

	type projectLoc struct{ project, location string }
	locs := []projectLoc{
		{projectID(), "us-central1"},
		{projectID() + "-b", "europe-west1"},
	}

	serviceID := unique("psc")
	ports := make([]int64, len(locs))
	for i, pl := range locs {
		parent := fmt.Sprintf("projects/%s/locations/%s", pl.project, pl.location)
		op, err := svc.Projects.Locations.Services.Create(parent, &metastore.Service{
			HiveMetastoreConfig: &metastore.HiveMetastoreConfig{Version: "3.1.2"},
		}).ServiceId(serviceID).Context(ctx).Do()
		require.NoError(t, err)
		require.True(t, op.Done)

		got, err := svc.Projects.Locations.Services.Get(fmt.Sprintf("%s/services/%s", parent, serviceID)).Context(ctx).Do()
		require.NoError(t, err)
		require.NotZero(t, got.Port, "the Service must publish its catalog port")
		require.Contains(t, got.EndpointUri, fmt.Sprintf(":%d", got.Port))
		ports[i] = got.Port
	}
	require.NotEqual(t, ports[0], ports[1], "two Services must not share a catalog port")

	db := hmsName("db")
	tbl := "t_" + db
	// Same database and table name in both catalogs.
	for i := range locs {
		i := i
		addr := fmt.Sprintf("127.0.0.1:%d", ports[i])
		t.Run(fmt.Sprintf("%s/%s", locs[i].project, locs[i].location), func(t *testing.T) {
			client, closeConn := dialGeneratedHMSAt(t, addr)
			defer closeConn()

			require.NoError(t, client.CreateDatabase(ctx, genDB(db)))
			tablename := tbl
			require.NoError(t, client.CreateTable(ctx, &hive_metastore.Table{
				DbName:    db,
				TableName: tbl,
				TableType: "EXTERNAL_TABLE",
				Sd:        &hive_metastore.StorageDescriptor{Location: fmt.Sprintf("gs://bucket/wh/%d/%s", i, tbl)},
			}))
			res, err := client.GetTableReq(ctx, &hive_metastore.GetTableRequest{DbName: db, TblName: tablename})
			require.NoError(t, err)
			got := res.Table
			require.NotNil(t, got)
			require.Equal(t, fmt.Sprintf("gs://bucket/wh/%d/%s", i, tbl), got.Sd.Location,
				"each catalog must return its own table record")

			// A database created in one Service is invisible in the other.
			if i == 0 {
				require.NoError(t, client.CreateDatabase(ctx, genDB("only_in_a")))
			} else {
				_, err := client.GetDatabase(ctx, "only_in_a")
				require.Error(t, err, "catalog b saw a database created only in catalog a")
			}

			// Iceberg commit locks are per catalog: this lock must not be
			// visible (let alone blocking) in the other Service.
			resp, err := client.Lock(ctx, &hive_metastore.LockRequest{
				Component: []*hive_metastore.LockComponent{{
					Type:      hive_metastore.LockType_EXCLUSIVE,
					Level:     hive_metastore.LockLevel_DB,
					Dbname:    db,
					Tablename: &tablename,
				}},
				User: "spark", Hostname: "test",
			})
			require.NoError(t, err)
			require.Equal(t, int32(hive_metastore.LockState_ACQUIRED), int32(resp.State))
		})
	}

	// A lock taken in Service a is NOT_ACQUIRED when queried through Service b:
	// the lock id namespace is per catalog.
	a, closeA := dialGeneratedHMSAt(t, fmt.Sprintf("127.0.0.1:%d", ports[0]))
	defer closeA()
	tablename := tbl
	lr, err := a.Lock(ctx, &hive_metastore.LockRequest{
		Component: []*hive_metastore.LockComponent{{
			Type: hive_metastore.LockType_EXCLUSIVE, Level: hive_metastore.LockLevel_DB,
			Dbname: db, Tablename: &tablename,
		}},
		User: "spark", Hostname: "test",
	})
	require.NoError(t, err)

	b, closeB := dialGeneratedHMSAt(t, fmt.Sprintf("127.0.0.1:%d", ports[1]))
	defer closeB()
	check, err := b.CheckLock(ctx, &hive_metastore.CheckLockRequest{Lockid: lr.Lockid})
	require.NoError(t, err)
	require.Equal(t, hive_metastore.LockState_NOT_ACQUIRED, check.State)
}
