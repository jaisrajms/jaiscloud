//go:build gcp_parity && gcp_conformance

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	admin "cloud.google.com/go/firestore/apiv1/admin"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	functionsv2 "cloud.google.com/go/functions/apiv2"
	"cloud.google.com/go/functions/apiv2/functionspb"
	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	pubsubapi "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	conf "jaiscloud/tests/gcpconformance"

	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// The pilot resources the generated field suite drives. Each adapter owns only
// the wire plumbing (paths, parents, LRO unwrapping); the field set and the
// values come from the generated dataset.

// preparedRefs resolves the generated reference values a resource needs against
// the real stores, so a reference field names a resource that exists (a KMS key
// validated by Firestore, a Pub/Sub topic a notification names).
func preparedRefs(ctx context.Context, e *Env, service, resource, transport string, have map[string]string) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range have {
		out[k] = v
	}
	need := map[string]bool{}
	for _, f := range generatedFieldsFor(service, resource, transport) {
		if f.Ref != "" {
			need[f.Ref] = true
		}
	}
	if need["kms"] {
		name, err := ensureRefCryptoKey(ctx, e)
		if err != nil {
			return nil, err
		}
		out["kms"] = name
	}
	if need["pubsub-topic"] {
		name, err := ensureRefTopic(ctx, e)
		if err != nil {
			return nil, err
		}
		out["pubsub-topic"] = name
	}
	if need["gcs-bucket"] {
		name, err := ensureRefBucket(ctx, e)
		if err != nil {
			return nil, err
		}
		out["gcs-bucket"] = name
	}
	return out, nil
}

func ensureRefCryptoKey(ctx context.Context, e *Env) (string, error) {
	ring := e.Resource("fc-ref-ring")
	key := e.Resource("fc-ref-key")
	base := fmt.Sprintf("/v1/projects/%s/locations/us", e.Cfg.Project)
	if _, err := e.Rest(ctx, http.MethodPost, base+"/keyRings?keyRingId="+ring, `{}`); err != nil && !statusIs(err, codes.AlreadyExists) {
		return "", fmt.Errorf("create reference key ring: %w", err)
	}
	ringName := fmt.Sprintf("projects/%s/locations/us/keyRings/%s", e.Cfg.Project, ring)
	if _, err := e.Rest(ctx, http.MethodPost, "/v1/"+ringName+"/cryptoKeys?cryptoKeyId="+key, `{"purpose":"ENCRYPT_DECRYPT"}`); err != nil && !statusIs(err, codes.AlreadyExists) {
		return "", fmt.Errorf("create reference crypto key: %w", err)
	}
	return ringName + "/cryptoKeys/" + key, nil
}

func ensureRefTopic(ctx context.Context, e *Env) (string, error) {
	id := e.Resource("fc-ref-topic")
	path := fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, id)
	if _, err := e.Rest(ctx, http.MethodPut, "/v1/"+path, `{}`); err != nil && !statusIs(err, codes.AlreadyExists) {
		return "", fmt.Errorf("create reference topic: %w", err)
	}
	return path, nil
}

func ensureRefBucket(ctx context.Context, e *Env) (string, error) {
	id := e.Resource("fc-ref-bucket")
	body := fmt.Sprintf(`{"name":%q,"location":"US"}`, id)
	if _, err := e.Rest(ctx, http.MethodPost, "/storage/v1/b?project="+url.QueryEscape(e.Cfg.Project), body); err != nil && !statusIs(err, codes.AlreadyExists) {
		return "", fmt.Errorf("create reference bucket: %w", err)
	}
	// The functions source fetcher reads the object the generated value names.
	object := "fc-object"
	_, status, _, err := e.RestRaw(ctx, http.MethodPost,
		"/upload/storage/v1/b/"+url.PathEscape(id)+"/o?uploadType=media&name="+url.QueryEscape(object),
		[]byte("exports.handler = () => {};"), "application/javascript", nil)
	if err != nil {
		return "", fmt.Errorf("upload reference object: %w", err)
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("upload reference object: HTTP %d", status)
	}
	return id, nil
}

// ─── Cloud KMS CryptoKey ─────────────────────────────────────────────────────

type kmsCryptoKey struct{}

func (kmsCryptoKey) resourceID() string { return "kms/CryptoKey" }
func (kmsCryptoKey) transports() []string {
	return []string{"rest", "grpc"}
}

func (kmsCryptoKey) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	ring := e.Resource("fc-ring")
	base := fmt.Sprintf("/v1/projects/%s/locations/global", e.Cfg.Project)
	if _, err := e.Rest(ctx, http.MethodPost, base+"/keyRings?keyRingId="+ring, `{}`); err != nil && !statusIs(err, codes.AlreadyExists) {
		return nil, fmt.Errorf("create key ring: %w", err)
	}
	return preparedRefs(ctx, e, "kms", "CryptoKey", "rest", nil)
}

func (kmsCryptoKey) baseBody(e *Env, id string) map[string]any {
	return map[string]any{"purpose": "ENCRYPT_DECRYPT"}
}

func (kmsCryptoKey) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{"purpose": "ENCRYPT_DECRYPT"}
}

func (kmsCryptoKey) fill(b *fieldBody, phase string) {
	if phase == "" {
		_ = b.placeAll()
	}
}

func (kmsCryptoKey) phases() []string { return []string{""} }

// deletable: the real KMS API has no crypto-key delete (a key is scheduled for
// destruction); the emulator exposes one as an extension, but the suite does
// not depend on it.
func (kmsCryptoKey) deletable() bool { return false }

func kmsKeyName(e *Env, id string) string {
	return fmt.Sprintf("projects/%s/locations/global/keyRings/%s/cryptoKeys/%s", e.Cfg.Project, e.Resource("fc-ring"), id)
}

func (kmsCryptoKey) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		ring := fmt.Sprintf("projects/%s/locations/global/keyRings/%s", e.Cfg.Project, e.Resource("fc-ring"))
		_, err = e.Rest(ctx, http.MethodPost, "/v1/"+ring+"/cryptoKeys?cryptoKeyId="+id, string(raw))
		return err
	case "grpc":
		msg := &kmspb.CryptoKey{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode crypto key: %w", err)
		}
		c, err := kms.NewKeyManagementClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent:      fmt.Sprintf("projects/%s/locations/global/keyRings/%s", e.Cfg.Project, e.Resource("fc-ring")),
			CryptoKeyId: id,
			CryptoKey:   msg,
		})
		return err
	}
	return fmt.Errorf("kms: unknown transport %q", transport)
}

func (kmsCryptoKey) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	name := kmsKeyName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+name, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := kms.NewKeyManagementClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: name})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("kms: unknown transport %q", transport)
}

func (kmsCryptoKey) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	ring := fmt.Sprintf("projects/%s/locations/global/keyRings/%s", e.Cfg.Project, e.Resource("fc-ring"))
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+ring+"/cryptoKeys", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "cryptoKeys", "name", kmsKeyName(e, id))
	case "grpc":
		c, err := kms.NewKeyManagementClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		it := c.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{Parent: ring})
		for {
			k, err := it.Next()
			if err == iterator.Done {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if k.GetName() == kmsKeyName(e, id) {
				return true, nil
			}
		}
	}
	return false, fmt.Errorf("kms: unknown transport %q", transport)
}

func (kmsCryptoKey) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	name := kmsKeyName(e, id)
	switch transport {
	case "rest":
		raw, err := marshalMerge(body)
		if err != nil {
			return err
		}
		_, err = e.Rest(ctx, http.MethodPatch, "/v1/"+name+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		raw, err := marshalMerge(body)
		if err != nil {
			return err
		}
		msg := &kmspb.CryptoKey{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode crypto key update: %w", err)
		}
		msg.Name = name
		c, err := kms.NewKeyManagementClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
			CryptoKey:  msg,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		return err
	}
	return fmt.Errorf("kms: unknown transport %q", transport)
}

func (kmsCryptoKey) delete(ctx context.Context, e *Env, transport, id string) error {
	name := kmsKeyName(e, id)
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/v1/"+name)
	case "grpc":
		c, err := kms.NewKeyManagementClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.DeleteCryptoKey(ctx, &kmspb.DeleteCryptoKeyRequest{Name: name})
		return err
	}
	return fmt.Errorf("kms: unknown transport %q", transport)
}

// ─── Firestore Database ──────────────────────────────────────────────────────

type firestoreDatabase struct{}

func (firestoreDatabase) resourceID() string { return "firestore/Database" }
func (firestoreDatabase) transports() []string {
	return []string{"rest", "grpc"}
}

func (firestoreDatabase) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	return preparedRefs(ctx, e, "firestore", "Database", "rest", nil)
}

func (firestoreDatabase) baseBody(e *Env, id string) map[string]any { return map[string]any{} }
func (firestoreDatabase) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{"locationId": "nam5"}
}
func (firestoreDatabase) fill(b *fieldBody, phase string) {
	if phase == "" {
		_ = b.placeAll()
	}
}

func (firestoreDatabase) phases() []string { return []string{""} }
func (firestoreDatabase) deletable() bool  { return true }

func firestoreDatabaseName(e *Env, id string) string {
	return fmt.Sprintf("projects/%s/databases/%s", e.Cfg.Project, id)
}

func (firestoreDatabase) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.RestOperationResource(ctx, http.MethodPost,
			fmt.Sprintf("/v1/projects/%s/databases?databaseId=%s", e.Cfg.Project, url.QueryEscape(id)), string(raw))
		return err
	case "grpc":
		msg := &adminpb.Database{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode database: %w", err)
		}
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:     "projects/" + e.Cfg.Project,
			DatabaseId: id,
			Database:   msg,
		})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	return fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreDatabase) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	name := firestoreDatabaseName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+name, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreDatabase) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	name := firestoreDatabaseName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/projects/"+e.Cfg.Project+"/databases", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "databases", "name", name)
	case "grpc":
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		resp, err := c.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: "projects/" + e.Cfg.Project})
		if err != nil {
			return false, err
		}
		for _, d := range resp.GetDatabases() {
			if d.GetName() == name {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreDatabase) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	name := firestoreDatabaseName(e, id)
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.RestOperationResource(ctx, http.MethodPatch, "/v1/"+name+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		msg := &adminpb.Database{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode database update: %w", err)
		}
		msg.Name = name
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   msg,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	return fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreDatabase) delete(ctx context.Context, e *Env, transport, id string) error {
	name := firestoreDatabaseName(e, id)
	switch transport {
	case "rest":
		// The REST delete takes the current etag as an OCC precondition.
		body, err := firestoreDatabase{}.read(ctx, e, "rest", id)
		if err != nil {
			return err
		}
		q := ""
		if etag, ok := body["etag"].(string); ok && etag != "" {
			q = "?etag=" + url.QueryEscape(etag)
		}
		return e.RestDelete(ctx, "/v1/"+name+q)
	case "grpc":
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: name})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	return fmt.Errorf("firestore: unknown transport %q", transport)
}

// ─── Firestore BackupSchedule ────────────────────────────────────────────────

type firestoreBackupSchedule struct{}

func (firestoreBackupSchedule) resourceID() string { return "firestore/BackupSchedule" }
func (firestoreBackupSchedule) transports() []string {
	return []string{"rest", "grpc"}
}

func (firestoreBackupSchedule) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	// A schedule lives under a database, and a database accepts at most one
	// daily schedule — so each create phase gets its own parent database.
	return preparedRefs(ctx, e, "firestore", "BackupSchedule", "rest", nil)
}

func (firestoreBackupSchedule) baseBody(e *Env, id string) map[string]any { return map[string]any{} }
func (firestoreBackupSchedule) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{"retention": "3600s", "dailyRecurrence": map[string]any{}}
}
func (firestoreBackupSchedule) fill(b *fieldBody, phase string) {
	if phase == "" {
		_ = b.placeAll()
	}
}

func (firestoreBackupSchedule) phases() []string { return []string{""} }
func (firestoreBackupSchedule) deletable() bool  { return true }

// backupScheduleParentID names the per-phase parent database: a database accepts
// only one daily schedule, so two creates cannot share a parent.
func backupScheduleParentID(id string) string { return "bsdb-" + id }

func backupScheduleParent(e *Env, id string) string {
	return firestoreDatabaseName(e, backupScheduleParentID(id))
}

// ensureBackupScheduleParent creates the parent database if it does not exist.
func ensureBackupScheduleParent(ctx context.Context, e *Env, id string) error {
	body := fmt.Sprintf(`{"locationId":"nam5"}`)
	_, err := e.RestOperationResource(ctx, http.MethodPost,
		fmt.Sprintf("/v1/projects/%s/databases?databaseId=%s", e.Cfg.Project, backupScheduleParentID(id)), body)
	if err != nil && !statusIs(err, codes.AlreadyExists) {
		return err
	}
	return nil
}

func (firestoreBackupSchedule) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	if err := ensureBackupScheduleParent(ctx, e, id); err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPost, "/v1/"+backupScheduleParent(e, id)+"/backupSchedules", string(raw))
		return err
	case "grpc":
		msg := &adminpb.BackupSchedule{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode backup schedule: %w", err)
		}
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
			Parent:         backupScheduleParent(e, id),
			BackupSchedule: msg,
		})
		return err
	}
	return fmt.Errorf("firestore: unknown transport %q", transport)
}

// backupScheduleName is the server-assigned schedule name: "1" for the first
// schedule on a fresh database.
func backupScheduleName(e *Env, id string) string {
	return backupScheduleParent(e, id) + "/backupSchedules/1"
}

func (firestoreBackupSchedule) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+backupScheduleName(e, id), "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: backupScheduleName(e, id)})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreBackupSchedule) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+backupScheduleParent(e, id)+"/backupSchedules", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "backupSchedules", "name", backupScheduleName(e, id))
	case "grpc":
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		resp, err := c.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: backupScheduleParent(e, id)})
		if err != nil {
			return false, err
		}
		for _, s := range resp.GetBackupSchedules() {
			if s.GetName() == backupScheduleName(e, id) {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreBackupSchedule) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPatch, "/v1/"+backupScheduleName(e, id)+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		msg := &adminpb.BackupSchedule{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode backup schedule update: %w", err)
		}
		msg.Name = backupScheduleName(e, id)
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
			BackupSchedule: msg,
			UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		return err
	}
	return fmt.Errorf("firestore: unknown transport %q", transport)
}

func (firestoreBackupSchedule) delete(ctx context.Context, e *Env, transport, id string) error {
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/v1/"+backupScheduleName(e, id))
	case "grpc":
		c, err := admin.NewFirestoreAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		return c.DeleteBackupSchedule(ctx, &adminpb.DeleteBackupScheduleRequest{Name: backupScheduleName(e, id)})
	}
	return fmt.Errorf("firestore: unknown transport %q", transport)
}

// ─── Cloud Functions v2 Function ─────────────────────────────────────────────

type functionsFunction struct{}

func (functionsFunction) resourceID() string { return "functions/Function" }
func (functionsFunction) transports() []string {
	return []string{"rest", "grpc"}
}

func (functionsFunction) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	return preparedRefs(ctx, e, "functions", "Function", "rest", nil)
}

func (functionsFunction) baseBody(e *Env, id string) map[string]any { return map[string]any{} }
func (functionsFunction) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{"buildConfig": map[string]any{"runtime": "nodejs20"}}
}
func (functionsFunction) fill(b *fieldBody, phase string) {
	if phase == "" {
		_ = b.placeAll()
	}
}

func (functionsFunction) phases() []string { return []string{""} }
func (functionsFunction) deletable() bool  { return true }

func functionsParent(e *Env) string {
	return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, functionsLocation)
}

func functionsName(e *Env, id string) string { return functionsParent(e) + "/functions/" + id }

func (functionsFunction) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.RestOperationResource(ctx, http.MethodPost, "/v2/"+functionsParent(e)+"/functions?functionId="+url.QueryEscape(id), string(raw))
		return err
	case "grpc":
		msg := &functionspb.Function{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode function: %w", err)
		}
		c, err := functionsv2.NewFunctionClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.CreateFunction(ctx, &functionspb.CreateFunctionRequest{Parent: functionsParent(e), FunctionId: id, Function: msg})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	return fmt.Errorf("functions: unknown transport %q", transport)
}

func (functionsFunction) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	name := functionsName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v2/"+name, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := functionsv2.NewFunctionClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: name})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("functions: unknown transport %q", transport)
}

func (functionsFunction) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	name := functionsName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v2/"+functionsParent(e)+"/functions", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "functions", "name", name)
	case "grpc":
		c, err := functionsv2.NewFunctionClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		it := c.ListFunctions(ctx, &functionspb.ListFunctionsRequest{Parent: functionsParent(e)})
		for {
			f, err := it.Next()
			if err == iterator.Done {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if f.GetName() == name {
				return true, nil
			}
		}
	}
	return false, fmt.Errorf("functions: unknown transport %q", transport)
}

func (functionsFunction) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	name := functionsName(e, id)
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.RestOperationResource(ctx, http.MethodPatch, "/v2/"+name+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		msg := &functionspb.Function{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode function update: %w", err)
		}
		msg.Name = name
		c, err := functionsv2.NewFunctionClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{
			Function:   msg,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	return fmt.Errorf("functions: unknown transport %q", transport)
}

func (functionsFunction) delete(ctx context.Context, e *Env, transport, id string) error {
	name := functionsName(e, id)
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/v2/"+name)
	case "grpc":
		c, err := functionsv2.NewFunctionClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: name})
		if err != nil {
			return err
		}
		return op.Wait(ctx)
	}
	return fmt.Errorf("functions: unknown transport %q", transport)
}

// ─── Cloud Monitoring AlertPolicy ────────────────────────────────────────────

type monitoringAlertPolicy struct{}

func (monitoringAlertPolicy) resourceID() string { return "monitoring/AlertPolicy" }
func (monitoringAlertPolicy) transports() []string {
	return []string{"rest", "grpc"}
}

func (monitoringAlertPolicy) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	return preparedRefs(ctx, e, "monitoring", "AlertPolicy", "rest", nil)
}

func (monitoringAlertPolicy) baseBody(e *Env, id string) map[string]any { return map[string]any{} }
func (monitoringAlertPolicy) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{
		"displayName": "fc-minimal",
		"conditions": []any{map[string]any{
			"displayName": "fc-minimal-condition",
			"conditionThreshold": map[string]any{
				"filter":         `metric.type = "custom.googleapis.com/fc/minimal"`,
				"comparison":     "COMPARISON_GT",
				"thresholdValue": 1,
			},
		}},
	}
}

// fill places the six mutually-exclusive condition variants as one element each:
// they share a oneof, so a single element cannot carry more than one. A few
// nested messages stop at the catalog's depth (their required sub-fields are
// below it) and are shaped explicitly.
func (monitoringAlertPolicy) fill(b *fieldBody, phase string) {
	if phase != "" {
		return
	}
	shapes := conditionShapes()
	var variants []string
	for _, f := range b.fields {
		if f.Kind != "message" || !strings.HasPrefix(f.Path, "conditions[].") {
			continue
		}
		variants = append(variants, f.Field)
	}
	sort.Strings(variants)
	for i, field := range variants {
		shape, ok := shapes[normalizeFieldPath(field)]
		if !ok {
			if b.placeErr == nil {
				b.placeErr = fmt.Errorf("no condition shape for %s", field)
			}
			continue
		}
		if err := b.shapeAt(field, i, shape); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
	}
	var conditionLeaves []string
	for _, f := range b.fields {
		if f.Kind == "message" || !strings.HasPrefix(f.Path, "conditions[].") {
			continue
		}
		conditionLeaves = append(conditionLeaves, f.Field)
	}
	for _, leaf := range conditionLeaves {
		for i := range variants {
			if err := b.placeAt(leaf, i); err != nil && b.placeErr == nil {
				b.placeErr = err
			}
		}
	}
	// Nested messages whose required members are below the catalog depth.
	nested := map[string]any{
		normalizeFieldPath("alertStrategy.notificationChannelStrategy"): []any{map[string]any{
			"notificationChannelNames": []any{"projects/fc-project/notificationChannels/fc-channel"},
			"renotifyInterval":         "600s",
		}},
		normalizeFieldPath("alertStrategy.notificationRateLimit"): map[string]any{"period": "300s"},
		normalizeFieldPath("documentation.links"): []any{map[string]any{
			"displayName": "fc-link", "url": "https://example.com/fc",
		}},
	}
	for _, f := range b.fields {
		v, ok := nested[normalizeFieldPath(f.Field)]
		if !ok || f.Kind != "message" {
			continue
		}
		if err := b.shapeValue(f.Field, v); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
	}
	// Everything outside conditions is placed by the generic pass.
	b.placeOutside("conditions")
}

func (monitoringAlertPolicy) phases() []string { return []string{""} }
func (monitoringAlertPolicy) deletable() bool  { return true }

// conditionShapes maps each condition oneof variant (normalized field path) to
// the minimal valid message body.
func conditionShapes() map[string]map[string]any {
	return map[string]map[string]any{
		normalizeFieldPath("conditions.conditionThreshold"): {
			"filter": `metric.type = "custom.googleapis.com/fc/threshold"`, "comparison": "COMPARISON_GT", "thresholdValue": 3.5,
		},
		normalizeFieldPath("conditions.conditionAbsent"): {
			"filter": `metric.type = "custom.googleapis.com/fc/absent"`, "duration": "60s",
		},
		normalizeFieldPath("conditions.conditionMatchedLog"): {
			"filter": `severity >= ERROR`,
		},
		normalizeFieldPath("conditions.conditionMonitoringQueryLanguage"): {
			"query":    `fetch gce_instance | metric 'custom.googleapis.com/fc/mql' | align rate(1m) | every 1m`,
			"duration": "60s",
		},
		normalizeFieldPath("conditions.conditionPrometheusQueryLanguage"): {
			"query":    `up == 0`,
			"duration": "60s",
		},
		normalizeFieldPath("conditions.conditionSql"): {
			"query": `SELECT 1`,
		},
	}
}

func alertPolicyName(e *Env, id string) string {
	return fmt.Sprintf("projects/%s/alertPolicies/%s", e.Cfg.Project, id)
}

func (monitoringAlertPolicy) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		created, cerr := e.Rest(ctx, http.MethodPost, "/v3/projects/"+e.Cfg.Project+"/alertPolicies", string(raw))
		if cerr != nil {
			return cerr
		}
		if body, derr := decodeJSONMap(created, nil); derr == nil {
			if name, _ := body["name"].(string); name != "" {
				rememberAlertPolicy(id, name)
			}
		}
		return nil
	case "grpc":
		msg := &monitoringpb.AlertPolicy{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode alert policy: %w", err)
		}
		c, err := monitoring.NewAlertPolicyClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		created, err := c.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{Name: "projects/" + e.Cfg.Project, AlertPolicy: msg})
		if err != nil {
			return err
		}
		rememberAlertPolicy(id, created.GetName())
		return nil
	}
	return fmt.Errorf("monitoring: unknown transport %q", transport)
}

// monitoringCreatedIDs remembers the server-assigned id per created policy, since
// the API assigns one and the generated id is only a run tag.
var (
	monitoringCreatedMu  sync.Mutex
	monitoringCreatedIDs = map[string]string{}
)

func rememberAlertPolicy(id, name string) {
	monitoringCreatedMu.Lock()
	defer monitoringCreatedMu.Unlock()
	monitoringCreatedIDs[id] = name
}

func monitoringPolicyName(e *Env, id string) string {
	monitoringCreatedMu.Lock()
	name := monitoringCreatedIDs[id]
	monitoringCreatedMu.Unlock()
	if name != "" {
		return name
	}
	return alertPolicyName(e, id)
}

func (monitoringAlertPolicy) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	name := monitoringPolicyName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v3/"+name, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := monitoring.NewAlertPolicyClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: name})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("monitoring: unknown transport %q", transport)
}

func (monitoringAlertPolicy) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	name := monitoringPolicyName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v3/projects/"+e.Cfg.Project+"/alertPolicies", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "alertPolicies", "name", name)
	case "grpc":
		c, err := monitoring.NewAlertPolicyClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		it := c.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{Name: "projects/" + e.Cfg.Project})
		for {
			p, err := it.Next()
			if err == iterator.Done {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if p.GetName() == name {
				return true, nil
			}
		}
	}
	return false, fmt.Errorf("monitoring: unknown transport %q", transport)
}

func (monitoringAlertPolicy) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	name := monitoringPolicyName(e, id)
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPatch, "/v3/"+name+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		msg := &monitoringpb.AlertPolicy{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode alert policy update: %w", err)
		}
		msg.Name = name
		c, err := monitoring.NewAlertPolicyClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
			AlertPolicy: msg,
			UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		return err
	}
	return fmt.Errorf("monitoring: unknown transport %q", transport)
}

func (monitoringAlertPolicy) delete(ctx context.Context, e *Env, transport, id string) error {
	name := monitoringPolicyName(e, id)
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/v3/"+name)
	case "grpc":
		c, err := monitoring.NewAlertPolicyClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		return c.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{Name: name})
	}
	return fmt.Errorf("monitoring: unknown transport %q", transport)
}

// ─── Cloud DNS ManagedZone (REST only) ───────────────────────────────────────

type clouddnsManagedZone struct{}

func (clouddnsManagedZone) resourceID() string { return "clouddns/ManagedZone" }
func (clouddnsManagedZone) transports() []string {
	return []string{"rest"}
}

func (clouddnsManagedZone) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	return preparedRefs(ctx, e, "clouddns", "ManagedZone", "rest", nil)
}

func (clouddnsManagedZone) baseBody(e *Env, id string) map[string]any {
	return map[string]any{"name": id, "dnsName": id + ".example.com."}
}

func (clouddnsManagedZone) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{"name": id, "dnsName": id + ".example.com."}
}

// fill keeps forwardingConfig and peeringConfig in separate zones: the API
// rejects a zone that carries both.
func (clouddnsManagedZone) fill(b *fieldBody, phase string) {
	if phase == "peering" {
		// peeringConfig requires a private zone; the phase carries the
		// visibility the block needs and nothing that would conflict.
		b.placePrefix("peeringConfig")
		if err := b.place("visibility"); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
		return
	}
	b.placeOutside("peeringConfig")
}

func (clouddnsManagedZone) phases() []string { return []string{"", "peering"} }
func (clouddnsManagedZone) deletable() bool  { return true }

func clouddnsZonePath(e *Env, id string) string {
	return fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s", e.Cfg.Project, id)
}

func (clouddnsManagedZone) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	_, err = e.Rest(ctx, http.MethodPost, "/dns/v1/projects/"+e.Cfg.Project+"/managedZones", string(raw))
	return err
}

func (clouddnsManagedZone) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	raw, err := e.Rest(ctx, http.MethodGet, clouddnsZonePath(e, id), "")
	body, err := decodeJSONMap(raw, err)
	if err != nil {
		return nil, err
	}
	if err := validateCloudDNSZone(body); err != nil {
		return nil, err
	}
	return body, nil
}

// validateCloudDNSZone runs the response through the vendored Discovery schema,
// the conformance validator Cloud DNS's REST-only surface has instead of a
// cross-transport diff.
func validateCloudDNSZone(body map[string]any) error {
	docs, err := conf.LoadSnapshots("../../tests/gcpconformance/discovery")
	if err != nil {
		return fmt.Errorf("load Discovery snapshots: %w", err)
	}
	doc, ok := docs["clouddns"]
	if !ok {
		return fmt.Errorf("vendored clouddns Discovery document is missing")
	}
	divs := conf.ValidateValue(doc, doc.Schemas["ManagedZone"], body, "managedZone")
	if len(divs) > 0 {
		return fmt.Errorf("Discovery validation at %s: %s (%s; want %s, got %s)", divs[0].Path, divs[0].Kind, divs[0].Severity, divs[0].Expected, divs[0].Actual)
	}
	return nil
}

func (clouddnsManagedZone) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	raw, err := e.Rest(ctx, http.MethodGet, "/dns/v1/projects/"+e.Cfg.Project+"/managedZones", "")
	if err != nil {
		return false, err
	}
	return jsonListContains(raw, "managedZones", "name", id)
}

func (clouddnsManagedZone) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	// The Cloud DNS PATCH replaces a configuration block; the suite sends the
	// whole block with the one changed leaf, which is what a client does.
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	_, err = e.Rest(ctx, http.MethodPatch, clouddnsZonePath(e, id), string(raw))
	return err
}

func (clouddnsManagedZone) delete(ctx context.Context, e *Env, transport, id string) error {
	return e.RestDelete(ctx, clouddnsZonePath(e, id))
}

// ─── Cloud Storage Bucket ────────────────────────────────────────────────────

type storageBucket struct{}

func (storageBucket) resourceID() string { return "storage/Bucket" }
func (storageBucket) transports() []string {
	return []string{"rest", "grpc"}
}

func (storageBucket) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	return preparedRefs(ctx, e, "storage", "Bucket", "rest", nil)
}

func (storageBucket) baseBody(e *Env, id string) map[string]any {
	return map[string]any{}
}

func (storageBucket) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{"name": id, "location": "US"}
}

func (storageBucket) fill(b *fieldBody, phase string) {
	if phase != "" {
		return
	}
	// The lifecycle rules are messages the catalog stops at (their action and
	// condition members are messages, not scalars), so they are shaped here.
	for _, field := range []string{"lifecycle.rule.action", "lifecycle.rule.condition"} {
		if _, ok := b.field(field); !ok {
			continue // not declared on this transport (lifecycle is REST-only today)
		}
		value := map[string]any{"type": "Delete"}
		if strings.HasSuffix(field, "condition") {
			value = map[string]any{"age": 30, "isLive": true, "matchesPrefix": []any{"fc-prefix/"}}
		}
		if err := b.shapeAt(field, 0, value); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
	}
	b.placeOutside("lifecycle")
	// The generic pass places everything outside the shaped block.
	for _, f := range b.fields {
		if f.Kind == "message" || b.used[f.Field] {
			continue
		}
		if strings.HasPrefix(f.Path, "lifecycle.") {
			continue
		}
		if err := b.placeAt(f.Field, 0); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
	}
}

func (storageBucket) phases() []string { return []string{""} }
func (storageBucket) deletable() bool  { return true }

func storageBucketName(e *Env, id string) string { return "projects/_/buckets/" + id }

func (storageBucket) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	switch transport {
	case "rest":
		body["name"] = id
		raw, err := marshalMerge(body)
		if err != nil {
			return err
		}
		_, err = e.Rest(ctx, http.MethodPost, "/storage/v1/b?project="+url.QueryEscape(e.Cfg.Project), string(raw))
		return err
	case "grpc":
		raw, err := marshalMerge(body)
		if err != nil {
			return err
		}
		bucket := &storagepb.Bucket{}
		if err := protojson.Unmarshal(raw, bucket); err != nil {
			return fmt.Errorf("decode bucket: %w", err)
		}
		return storageDial(ctx, e, func(c storagepb.StorageClient) error {
			_, cerr := c.CreateBucket(ctx, &storagepb.CreateBucketRequest{
				Parent:   "projects/" + e.Cfg.Project,
				BucketId: id,
				Bucket:   bucket,
			})
			return cerr
		})
	}
	return fmt.Errorf("storage: unknown transport %q", transport)
}

func (storageBucket) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/storage/v1/b/"+id, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		var bucket *storagepb.Bucket
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			bucket, cerr = c.GetBucket(ctx, &storagepb.GetBucketRequest{Name: storageBucketName(e, id)})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return protoMap(bucket)
	}
	return nil, fmt.Errorf("storage: unknown transport %q", transport)
}

func (storageBucket) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/storage/v1/b?project="+url.QueryEscape(e.Cfg.Project), "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "items", "name", id)
	case "grpc":
		var found bool
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			resp, cerr := c.ListBuckets(ctx, &storagepb.ListBucketsRequest{Parent: "projects/" + e.Cfg.Project})
			if cerr != nil {
				return cerr
			}
			for _, b := range resp.GetBuckets() {
				if b.GetName() == storageBucketName(e, id) {
					found = true
				}
			}
			return nil
		})
		return found, err
	}
	return false, fmt.Errorf("storage: unknown transport %q", transport)
}

func (storageBucket) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		// Buckets.patch merges; GCS has no update mask on the v1 surface.
		_, err = e.Rest(ctx, http.MethodPatch, "/storage/v1/b/"+id, string(raw))
		return err
	case "grpc":
		bucket := &storagepb.Bucket{}
		if err := protojson.Unmarshal(raw, bucket); err != nil {
			return fmt.Errorf("decode bucket update: %w", err)
		}
		bucket.Name = storageBucketName(e, id)
		return storageDial(ctx, e, func(c storagepb.StorageClient) error {
			_, cerr := c.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
				Bucket:     bucket,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}},
			})
			return cerr
		})
	}
	return fmt.Errorf("storage: unknown transport %q", transport)
}

func (storageBucket) delete(ctx context.Context, e *Env, transport, id string) error {
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/storage/v1/b/"+id)
	case "grpc":
		return storageDial(ctx, e, func(c storagepb.StorageClient) error {
			_, cerr := c.DeleteBucket(ctx, &storagepb.DeleteBucketRequest{Name: storageBucketName(e, id)})
			return cerr
		})
	}
	return fmt.Errorf("storage: unknown transport %q", transport)
}

// ─── Pub/Sub Topic (REST + gRPC) ─────────────────────────────────────────────

type pubsubTopic struct{}

func (pubsubTopic) resourceID() string { return "pubsub/Topic" }
func (pubsubTopic) transports() []string {
	return []string{"rest", "grpc"}
}

func (pubsubTopic) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	// The REST Discovery schema carries no resource_reference annotations, so
	// the reference fields are resolved from both transports' generated sets.
	refs := map[string]string{}
	for _, transport := range []string{"rest", "grpc"} {
		r, err := preparedRefs(ctx, e, "pubsub", "Topic", transport, refs)
		if err != nil {
			return nil, err
		}
		refs = r
	}
	return refs, nil
}

func (pubsubTopic) baseBody(e *Env, id string) map[string]any { return map[string]any{} }
func (pubsubTopic) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{}
}

// phases: the ingestion source is a oneof and the transform list is a repeated
// oneof, so each source and each transform variant gets its own create (the
// catalog stops at depth 2, where those members are messages).
func (pubsubTopic) phases() []string {
	return []string{"", "aws-kinesis", "aws-msk", "azure", "cloud-storage", "confluent", "compression", "ai-inference"}
}

func (pubsubTopic) deletable() bool { return true }

func (pubsubTopic) fill(b *fieldBody, phase string) {
	switch phase {
	case "":
		// Everything except the ingestion block and the non-javascript transform
		// variants (those are oneof members and get their own phase).
		for _, f := range b.fields {
			if f.Kind == "message" {
				continue
			}
			np := normalizeFieldPath(f.Path)
			switch {
			case strings.HasPrefix(np, "ingestiondatasourcesettings"):
				continue
			case strings.HasPrefix(np, "messagetransforms"):
				if strings.Contains(np, "compression") || strings.Contains(np, "aiinference") {
					continue
				}
			}
			if err := b.place(f.Field); err != nil && b.placeErr == nil {
				b.placeErr = err
			}
		}
		shape(b, "messageTransforms.javascriptUdf", map[string]any{
			"functionName": "fc-transform",
			"code":         "function fcTransform(m) { return m; }",
		})
	case "compression":
		shape(b, "messageTransforms.compression", map[string]any{
			"compressionAlgorithm": "ZLIB",
			"compressionMode":      "COMPRESS",
		})
	case "ai-inference":
		shape(b, "messageTransforms.aiInference", map[string]any{
			"endpoint":              "projects/fc-project/locations/us-central1/endpoints/fc-endpoint",
			"serviceAccountEmail":   "fc-inference@fc-project.iam.gserviceaccount.com",
			"unstructuredInference": map[string]any{},
		})
	case "aws-kinesis":
		shape(b, "ingestionDataSourceSettings.awsKinesis", map[string]any{
			"awsRoleArn":        "arn:aws:iam::123456789012:role/fc",
			"consumerArn":       "arn:aws:kinesis:us-east-1:123456789012:stream/fc/consumer/fc:1",
			"gcpServiceAccount": "fc-ingest@fc-project.iam.gserviceaccount.com",
			"streamArn":         "arn:aws:kinesis:us-east-1:123456789012:stream/fc",
		})
		shape(b, "ingestionDataSourceSettings.platformLogsSettings", map[string]any{"severity": "INFO"})
	case "aws-msk":
		shape(b, "ingestionDataSourceSettings.awsMsk", map[string]any{
			"awsRoleArn":        "arn:aws:iam::123456789012:role/fc",
			"clusterArn":        "arn:aws:kafka:us-east-1:123456789012:cluster/fc/1",
			"gcpServiceAccount": "fc-ingest@fc-project.iam.gserviceaccount.com",
			"topic":             b.refs["pubsub-topic"],
		})
	case "azure":
		shape(b, "ingestionDataSourceSettings.azureEventHubs", map[string]any{
			"clientId":          "fc-client",
			"eventHub":          "fc-hub",
			"gcpServiceAccount": "fc-ingest@fc-project.iam.gserviceaccount.com",
			"namespace":         "fc-namespace",
			"resourceGroup":     "fc-resource-group",
			"subscriptionId":    "fc-subscription",
			"tenantId":          "fc-tenant",
		})
	case "cloud-storage":
		shape(b, "ingestionDataSourceSettings.cloudStorage", map[string]any{
			"bucket":                  "fc-ingest-bucket",
			"matchGlob":               "*.txt",
			"minimumObjectCreateTime": "2030-01-01T00:00:00Z",
		})
	case "confluent":
		shape(b, "ingestionDataSourceSettings.confluentCloud", map[string]any{
			"bootstrapServer":   "fc.confluent.cloud:9092",
			"clusterId":         "fc-cluster",
			"gcpServiceAccount": "fc-ingest@fc-project.iam.gserviceaccount.com",
			"identityPoolId":    "fc-pool",
			"topic":             "fc-confluent-topic",
		})
	}
}

// shape places a message row the catalog stops at with the sub-fields the API
// requires, ignoring a field that this transport does not declare. The name may
// be given in either transport's spelling; the generated field is resolved by
// its normalized path.
func shape(b *fieldBody, field string, value map[string]any) {
	want := normalizeFieldPath(field)
	for _, f := range b.fields {
		if f.Kind != "message" || normalizeFieldPath(f.Field) != want {
			continue
		}
		if err := b.shape(f.Field, value); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
		return
	}
}

// placeField places a scalar field by its normalized name, so a driver can name
// a field once for both transports.
func placeField(b *fieldBody, field string) {
	want := normalizeFieldPath(field)
	for _, f := range b.fields {
		if normalizeFieldPath(f.Field) != want {
			continue
		}
		if err := b.place(f.Field); err != nil && b.placeErr == nil {
			b.placeErr = err
		}
		return
	}
}

func pubsubTopicName(e *Env, id string) string {
	return fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, id)
}

func (pubsubTopic) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPut, "/v1/"+pubsubTopicName(e, id), string(raw))
		return err
	case "grpc":
		msg := &pubsubpb.Topic{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode topic: %w", err)
		}
		msg.Name = pubsubTopicName(e, id)
		c, err := pubsubapi.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.CreateTopic(ctx, msg)
		return err
	}
	return fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubTopic) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	name := pubsubTopicName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+name, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := pubsubapi.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubTopic) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	name := pubsubTopicName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/projects/"+e.Cfg.Project+"/topics", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "topics", "name", name)
	case "grpc":
		c, err := pubsubapi.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		it := c.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + e.Cfg.Project})
		for {
			t, err := it.Next()
			if err == iterator.Done {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if t.GetName() == name {
				return true, nil
			}
		}
	}
	return false, fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubTopic) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	name := pubsubTopicName(e, id)
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPatch, "/v1/"+name+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		msg := &pubsubpb.Topic{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode topic update: %w", err)
		}
		msg.Name = name
		c, err := pubsubapi.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
			Topic:      msg,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		return err
	}
	return fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubTopic) delete(ctx context.Context, e *Env, transport, id string) error {
	name := pubsubTopicName(e, id)
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/v1/"+name)
	case "grpc":
		c, err := pubsubapi.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		return c.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: name})
	}
	return fmt.Errorf("pubsub: unknown transport %q", transport)
}

// ─── Pub/Sub Subscription (REST + gRPC) ──────────────────────────────────────

type pubsubSubscription struct{}

func (pubsubSubscription) resourceID() string { return "pubsub/Subscription" }
func (pubsubSubscription) transports() []string {
	return []string{"rest", "grpc"}
}

func (pubsubSubscription) prepare(ctx context.Context, e *Env) (map[string]string, error) {
	refs := map[string]string{}
	for _, transport := range []string{"rest", "grpc"} {
		r, err := preparedRefs(ctx, e, "pubsub", "Subscription", transport, refs)
		if err != nil {
			return nil, err
		}
		refs = r
	}
	return refs, nil
}

func (pubsubSubscription) topicName(e *Env) string {
	return fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, e.Resource("fc-ref-topic"))
}

func (s pubsubSubscription) baseBody(e *Env, id string) map[string]any {
	return map[string]any{"topic": s.topicName(e)}
}

// absenceBody carries the two API defaults the emulator always renders (a
// subscription without an explicit policy expires after 31 days); every other
// optional block must stay absent.
func (s pubsubSubscription) absenceBody(e *Env, id string) map[string]any {
	return map[string]any{
		"topic":            s.topicName(e),
		"expirationPolicy": map[string]any{"ttl": "2678400s"},
	}
}

// phases: the Cloud Storage output format and the push wrapper are oneofs, and
// the transform list is a repeated oneof, so each variant gets its own create.
func (pubsubSubscription) phases() []string {
	return []string{"", "avro", "no-wrapper", "pubsub-wrapper"}
}

func (pubsubSubscription) deletable() bool { return true }

func (pubsubSubscription) fill(b *fieldBody, phase string) {
	switch phase {
	case "":
		// Pull subscription: everything except the push and Cloud Storage output
		// blocks (each is a oneof, and exactly-once delivery is rejected on a
		// push subscription), plus the javascript transform variant.
		for _, f := range b.fields {
			if f.Kind == "message" {
				continue
			}
			np := normalizeFieldPath(f.Path)
			switch {
			case strings.HasPrefix(np, "pushconfig"):
				continue
			case strings.HasPrefix(np, "cloudstorageconfigavroconfig"):
				continue
			case strings.HasPrefix(np, "messagetransforms"):
				if strings.Contains(np, "compression") || strings.Contains(np, "aiinference") {
					continue
				}
			}
			if err := b.place(f.Field); err != nil && b.placeErr == nil {
				b.placeErr = err
			}
		}
		shape(b, "messageTransforms.javascriptUdf", map[string]any{
			"functionName": "fc-transform",
			"code":         "function fcTransform(m) { return m; }",
		})
		shape(b, "cloudStorageConfig.textConfig", map[string]any{})
	case "avro":
		placeField(b, "cloudStorageConfig.bucket")
		shape(b, "cloudStorageConfig.avroConfig", map[string]any{})
	case "no-wrapper":
		// A push subscription cannot also enable exactly-once delivery, so the
		// push fields live in their own create phase.
		placeField(b, "pushConfig.pushEndpoint")
		placeField(b, "pushConfig.attributes")
		shape(b, "pushConfig.oidcToken", map[string]any{
			"audience":            "fc-audience",
			"serviceAccountEmail": "fc-push@fc-project.iam.gserviceaccount.com",
		})
		shape(b, "pushConfig.noWrapper", map[string]any{"writeMetadata": true})
		shape(b, "messageTransforms.compression", map[string]any{
			"compressionAlgorithm": "ZLIB",
			"compressionMode":      "COMPRESS",
		})
	case "pubsub-wrapper":
		placeField(b, "pushConfig.pushEndpoint")
		shape(b, "pushConfig.pubsubWrapper", map[string]any{})
		shape(b, "messageTransforms.aiInference", map[string]any{
			"endpoint":              "projects/fc-project/locations/us-central1/endpoints/fc-endpoint",
			"serviceAccountEmail":   "fc-inference@fc-project.iam.gserviceaccount.com",
			"unstructuredInference": map[string]any{},
		})
	}
}

func pubsubSubscriptionName(e *Env, id string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", e.Cfg.Project, id)
}

func (pubsubSubscription) create(ctx context.Context, e *Env, transport, id string, body map[string]any) error {
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPut, "/v1/"+pubsubSubscriptionName(e, id), string(raw))
		return err
	case "grpc":
		msg := &pubsubpb.Subscription{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode subscription: %w", err)
		}
		msg.Name = pubsubSubscriptionName(e, id)
		c, err := pubsubapi.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.CreateSubscription(ctx, msg)
		return err
	}
	return fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubSubscription) read(ctx context.Context, e *Env, transport, id string) (map[string]any, error) {
	name := pubsubSubscriptionName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/"+name, "")
		return decodeJSONMap(raw, err)
	case "grpc":
		c, err := pubsubapi.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		msg, err := c.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
		if err != nil {
			return nil, err
		}
		return protoMap(msg)
	}
	return nil, fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubSubscription) listContains(ctx context.Context, e *Env, transport, id string) (bool, error) {
	name := pubsubSubscriptionName(e, id)
	switch transport {
	case "rest":
		raw, err := e.Rest(ctx, http.MethodGet, "/v1/projects/"+e.Cfg.Project+"/subscriptions", "")
		if err != nil {
			return false, err
		}
		return jsonListContains(raw, "subscriptions", "name", name)
	case "grpc":
		c, err := pubsubapi.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return false, err
		}
		defer c.Close()
		it := c.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: "projects/" + e.Cfg.Project})
		for {
			s, err := it.Next()
			if err == iterator.Done {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if s.GetName() == name {
				return true, nil
			}
		}
	}
	return false, fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubSubscription) update(ctx context.Context, e *Env, transport, id, mask string, body map[string]any) error {
	name := pubsubSubscriptionName(e, id)
	raw, err := marshalMerge(body)
	if err != nil {
		return err
	}
	switch transport {
	case "rest":
		_, err = e.Rest(ctx, http.MethodPatch, "/v1/"+name+"?updateMask="+url.QueryEscape(mask), string(raw))
		return err
	case "grpc":
		msg := &pubsubpb.Subscription{}
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode subscription update: %w", err)
		}
		msg.Name = name
		c, err := pubsubapi.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
			Subscription: msg,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		return err
	}
	return fmt.Errorf("pubsub: unknown transport %q", transport)
}

func (pubsubSubscription) delete(ctx context.Context, e *Env, transport, id string) error {
	name := pubsubSubscriptionName(e, id)
	switch transport {
	case "rest":
		return e.RestDelete(ctx, "/v1/"+name)
	case "grpc":
		c, err := pubsubapi.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		return c.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: name})
	}
	return fmt.Errorf("pubsub: unknown transport %q", transport)
}

// ─── shared plumbing ─────────────────────────────────────────────────────────

func marshalMerge(body map[string]any) ([]byte, error) {
	return json.Marshal(body)
}

func decodeJSONMap(raw json.RawMessage, err error) (map[string]any, error) {
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return out, nil
}

// protoMap renders a proto response with proto field names, so it matches the
// generated gRPC field paths.
func protoMap(m proto.Message) (map[string]any, error) {
	if m == nil {
		return nil, fmt.Errorf("nil proto response")
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("protojson marshal: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode protojson: %w", err)
	}
	return out, nil
}

// jsonListContains reports whether a list envelope carries an element whose
// identifier equals want.
func jsonListContains(raw json.RawMessage, arrayKey, idKey, want string) (bool, error) {
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		return false, fmt.Errorf("decode list: %w", err)
	}
	arr, _ := env[arrayKey].([]any)
	for _, el := range arr {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := m[idKey].(string); id == want {
			return true, nil
		}
	}
	return false, nil
}

// fieldResources is the pilot set the generated suite drives, in a stable order.
func fieldResources() []fieldResource {
	return []fieldResource{
		kmsCryptoKey{},
		firestoreDatabase{},
		firestoreBackupSchedule{},
		functionsFunction{},
		monitoringAlertPolicy{},
		clouddnsManagedZone{},
		pubsubTopic{},
		pubsubSubscription{},
		storageBucket{},
	}
}
