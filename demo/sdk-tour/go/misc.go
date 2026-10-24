package main

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	kms "cloud.google.com/go/kms/apiv1"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/storage"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// grpcOptions is the plaintext, no-auth wiring every generated gRPC client that
// has no emulator env hook needs.
func grpcOptions(cfg Config) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(cfg.GRPC),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

func isConflict(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == 409
}

// ─── Cloud Logging (generated gRPC client; no emulator env hook) ──────────────

func loggingScenarios(r *runner, f *fixtures) {
	parent := "projects/" + r.cfg.Project
	logID := rid(r.cfg, "sdk-tour-log")
	logName := parent + "/logs/" + logID

	write := func(ctx context.Context, client *logging.Client, text string) error {
		_, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
			LogName: logName,
			Resource: &mrpb.MonitoredResource{
				Type:   "global",
				Labels: map[string]string{"project_id": r.cfg.Project},
			},
			Entries: []*loggingpb.LogEntry{{
				LogName:  logName,
				Severity: ltype.LogSeverity_INFO,
				Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: text},
			}},
		})
		return err
	}

	r.run("logging.write_entry", "OK", func(ctx context.Context) (string, string, error) {
		client, err := logging.NewClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", fmt.Errorf("new logging client: %w", err)
		}
		defer client.Close()
		if err := write(ctx, client, "sdk-tour-entry"); err != nil {
			return "", "", fmt.Errorf("write log entry: %w", err)
		}
		it := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{parent},
			Filter:        fmt.Sprintf("logName=%q", logName),
		})
		n := 0
		for {
			_, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return "", "", fmt.Errorf("list log entries: %w", err)
			}
			n++
		}
		if n != 1 {
			return "", "", fmt.Errorf("ListLogEntries returned %d entries, want 1", n)
		}
		return fmt.Sprintf("%d", n), "WriteLogEntries + ListLogEntries round-trip", nil
	})

	r.run("logging.tail", "OK", func(ctx context.Context) (string, string, error) {
		client, err := logging.NewClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", fmt.Errorf("new logging client: %w", err)
		}
		defer client.Close()
		// The emulator seeds its "new since stream start" cursor when it reads
		// the opening request, so the entry must be written AFTER the stream is
		// open. Write on a timer (retrying) until the tail delivers one; this
		// tolerates the initial-request race instead of flaking on it.
		stream, err := client.TailLogEntries(ctx)
		if err != nil {
			return "", "", fmt.Errorf("TailLogEntries: %w", err)
		}
		if err := stream.Send(&loggingpb.TailLogEntriesRequest{
			ResourceNames: []string{parent},
			Filter:        fmt.Sprintf("logName=%q", logName),
			BufferWindow:  durationpb.New(200 * time.Millisecond),
		}); err != nil {
			return "", "", fmt.Errorf("tail send: %w", err)
		}
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(400 * time.Millisecond):
			}
			for i := 0; i < 40; i++ {
				_ = write(ctx, client, fmt.Sprintf("sdk-tour-tail-%d", i))
				select {
				case <-ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
		}()
		for {
			resp, err := stream.Recv()
			if err != nil {
				return "", "", fmt.Errorf("tail recv: %w", err)
			}
			for _, e := range resp.GetEntries() {
				if strings.HasPrefix(e.GetTextPayload(), "sdk-tour-tail") {
					return "true", "TailLogEntries delivered the entry", nil
				}
			}
		}
	})
}

// ─── Secret Manager ──────────────────────────────────────────────────────────

func secretScenarios(r *runner, f *fixtures) {
	parent := "projects/" + r.cfg.Project
	secretID := rid(r.cfg, "sdk-tour-secret")

	r.run("secretmanager.add_access_list", "OK", func(ctx context.Context) (string, string, error) {
		client, err := secretmanager.NewClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", fmt.Errorf("new secret client: %w", err)
		}
		defer client.Close()
		secret, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
			Parent:   parent,
			SecretId: secretID,
			Secret: &secretmanagerpb.Secret{
				Replication: &secretmanagerpb.Replication{
					Replication: &secretmanagerpb.Replication_Automatic_{
						Automatic: &secretmanagerpb.Replication_Automatic{},
					},
				},
			},
		})
		if err != nil {
			return "", "", fmt.Errorf("create secret: %w", err)
		}
		if _, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent:  secret.Name,
			Payload: &secretmanagerpb.SecretPayload{Data: []byte("sdk-tour")},
		}); err != nil {
			return "", "", fmt.Errorf("add version: %w", err)
		}
		acc, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
			Name: secret.Name + "/versions/1",
		})
		if err != nil {
			return "", "", fmt.Errorf("access version: %w", err)
		}
		if string(acc.GetPayload().GetData()) != "sdk-tour" {
			return "", "", fmt.Errorf("accessed payload = %q, want %q", acc.GetPayload().GetData(), "sdk-tour")
		}
		it := client.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: secret.Name})
		versions := 0
		for {
			_, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return "", "", fmt.Errorf("list versions: %w", err)
			}
			versions++
		}
		if versions < 1 {
			return "", "", fmt.Errorf("ListSecretVersions returned %d versions", versions)
		}
		return fmt.Sprintf("%d", versions), fmt.Sprintf("access=ok versions=%d", versions), nil
	})
}

// ─── Cloud KMS ───────────────────────────────────────────────────────────────

func kmsScenarios(r *runner, f *fixtures) {
	parent := "projects/" + r.cfg.Project + "/locations/global"

	r.run("kms.encrypt_decrypt", "OK", func(ctx context.Context) (string, string, error) {
		client, err := kms.NewKeyManagementClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", fmt.Errorf("new kms client: %w", err)
		}
		defer client.Close()
		ring, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
			Parent: parent, KeyRingId: rid(r.cfg, "sdk-tour-ring"),
			KeyRing: &kmspb.KeyRing{},
		})
		if err != nil {
			return "", "", fmt.Errorf("create keyring: %w", err)
		}
		key, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent: ring.Name, CryptoKeyId: rid(r.cfg, "sdk-tour-key"),
			CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		if err != nil {
			return "", "", fmt.Errorf("create key: %w", err)
		}
		enc, err := client.Encrypt(ctx, &kmspb.EncryptRequest{Name: key.Name, Plaintext: []byte("sdk-tour")})
		if err != nil {
			return "", "", fmt.Errorf("encrypt: %w", err)
		}
		dec, err := client.Decrypt(ctx, &kmspb.DecryptRequest{Name: key.Name, Ciphertext: enc.GetCiphertext()})
		if err != nil {
			return "", "", fmt.Errorf("decrypt: %w", err)
		}
		if string(dec.GetPlaintext()) != "sdk-tour" {
			return "", "", fmt.Errorf("decrypted = %q, want %q", dec.GetPlaintext(), "sdk-tour")
		}
		return "true", "encrypt/decrypt round-trip", nil
	})

	r.run("kms.asymmetric_sign", "OK", func(ctx context.Context) (string, string, error) {
		client, err := kms.NewKeyManagementClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", fmt.Errorf("new kms client: %w", err)
		}
		defer client.Close()
		ring, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
			Parent: parent, KeyRingId: rid(r.cfg, "sdk-tour-sign-ring"),
			KeyRing: &kmspb.KeyRing{},
		})
		if err != nil {
			return "", "", fmt.Errorf("create keyring: %w", err)
		}
		key, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent: ring.Name, CryptoKeyId: rid(r.cfg, "sdk-tour-sign-key"),
			CryptoKey: &kmspb.CryptoKey{
				Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN,
				VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
					Algorithm: kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256,
				},
			},
		})
		if err != nil {
			return "", "", fmt.Errorf("create sign key: %w", err)
		}
		version := key.Name + "/cryptoKeyVersions/1"
		digest := sha256.Sum256([]byte("sdk-tour-sign"))
		sig, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
			Name:   version,
			Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
		})
		if err != nil {
			return "", "", fmt.Errorf("asymmetric sign: %w", err)
		}
		pk, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: version})
		if err != nil {
			return "", "", fmt.Errorf("get public key: %w", err)
		}
		block, _ := pem.Decode([]byte(pk.GetPem()))
		if block == nil {
			return "", "", fmt.Errorf("public key is not PEM")
		}
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return "", "", fmt.Errorf("parse public key: %w", err)
		}
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return "", "", fmt.Errorf("public key is %T, want *rsa.PublicKey", pub)
		}
		if err := rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, digest[:], sig.GetSignature()); err != nil {
			return "", "", fmt.Errorf("signature did not verify: %w", err)
		}
		return "true", "AsymmetricSign verified with GetPublicKey", nil
	})
}

// ─── BigQuery (REST apiary client) ───────────────────────────────────────────

func newBigQueryService(ctx context.Context, cfg Config) (*bigquery.Service, error) {
	return bigquery.NewService(ctx,
		option.WithEndpoint(cfg.REST+"/"),
		option.WithoutAuthentication(),
	)
}

func bqSchema() *bigquery.TableSchema {
	return &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "STRING"},
	}}
}

func bigqueryScenarios(r *runner, f *fixtures) {
	project := r.cfg.Project
	dsID := strings.ReplaceAll(rid(r.cfg, "sdk_tour_ds"), "-", "_")
	tblID := strings.ReplaceAll(rid(r.cfg, "sdk_tour_tbl"), "-", "_")

	ensureDataset := func(svc *bigquery.Service) error {
		if _, err := svc.Datasets.Insert(project, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: dsID},
			FriendlyName:     "sdk-tour",
		}).Do(); err != nil && !isConflict(err) {
			return fmt.Errorf("create dataset: %w", err)
		}
		return nil
	}
	ensureTable := func(svc *bigquery.Service, tbl string) error {
		if _, err := svc.Tables.Insert(project, dsID, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: dsID, TableId: tbl},
			Schema:         bqSchema(),
		}).Do(); err != nil && !isConflict(err) {
			return fmt.Errorf("create table: %w", err)
		}
		return nil
	}

	r.run("bigquery.insertall_query", "OK", func(ctx context.Context) (string, string, error) {
		svc, err := newBigQueryService(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		if err := ensureDataset(svc); err != nil {
			return "", "", err
		}
		if err := ensureTable(svc, tblID); err != nil {
			return "", "", err
		}
		if _, err := svc.Tabledata.InsertAll(project, dsID, tblID, &bigquery.TableDataInsertAllRequest{
			Rows: []*bigquery.TableDataInsertAllRequestRows{
				{InsertId: "1", Json: map[string]bigquery.JsonValue{"id": 1, "name": "alice"}},
				{InsertId: "2", Json: map[string]bigquery.JsonValue{"id": 2, "name": "bob"}},
			},
		}).Do(); err != nil {
			return "", "", fmt.Errorf("insertAll: %w", err)
		}
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        fmt.Sprintf("SELECT id, name FROM `%s.%s.%s` ORDER BY id", project, dsID, tblID),
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		if err != nil {
			return "", "", fmt.Errorf("query: %w", err)
		}
		if len(qr.Rows) != 2 {
			return "", "", fmt.Errorf("query returned %d rows, want 2", len(qr.Rows))
		}
		return fmt.Sprintf("%d", len(qr.Rows)), "insertAll + Jobs.Query returned 2 rows", nil
	})

	r.run("bigquery.load_job", "OK", func(ctx context.Context) (string, string, error) {
		sc, err := newStorageClient(ctx)
		if err != nil {
			return "", "", err
		}
		defer sc.Close()
		if err := sc.Bucket(f.bigBucket).Create(ctx, project, &storage.BucketAttrs{Location: "US"}); err != nil && !isConflict(err) {
			return "", "", fmt.Errorf("create load bucket: %w", err)
		}
		w := sc.Bucket(f.bigBucket).Object("rows.json").NewWriter(ctx)
		w.ContentType = "application/x-ndjson"
		if _, err := w.Write([]byte("{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n")); err != nil {
			return "", "", err
		}
		if err := w.Close(); err != nil {
			return "", "", err
		}

		svc, err := newBigQueryService(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		loadTbl := strings.ReplaceAll(rid(r.cfg, "sdk_tour_load"), "-", "_")
		if err := ensureDataset(svc); err != nil {
			return "", "", err
		}
		if err := ensureTable(svc, loadTbl); err != nil {
			return "", "", err
		}
		jobID := strings.ReplaceAll(rid(r.cfg, "sdk_tour_loadjob"), "-", "_")
		if _, err := svc.Jobs.Insert(project, &bigquery.Job{
			JobReference: &bigquery.JobReference{ProjectId: project, JobId: jobID},
			Configuration: &bigquery.JobConfiguration{Load: &bigquery.JobConfigurationLoad{
				SourceUris:       []string{fmt.Sprintf("gs://%s/rows.json", f.bigBucket)},
				DestinationTable: &bigquery.TableReference{ProjectId: project, DatasetId: dsID, TableId: loadTbl},
				SourceFormat:     "NEWLINE_DELIMITED_JSON",
				WriteDisposition: "WRITE_APPEND",
			}},
		}).Do(); err != nil {
			return "", "", fmt.Errorf("insert load job: %w", err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := svc.Jobs.Get(project, jobID).Do()
			if err != nil {
				return "", "", fmt.Errorf("get load job: %w", err)
			}
			if got.Status != nil && got.Status.State == "DONE" {
				break
			}
			if time.Now().After(deadline) {
				return "", "", fmt.Errorf("load job did not complete")
			}
			time.Sleep(200 * time.Millisecond)
		}
		data, err := svc.Tabledata.List(project, dsID, loadTbl).Do()
		if err != nil {
			return "", "", fmt.Errorf("list loaded rows: %w", err)
		}
		if len(data.Rows) != 2 {
			return "", "", fmt.Errorf("load job produced %d rows, want 2", len(data.Rows))
		}
		return fmt.Sprintf("%d", len(data.Rows)), "gs:// load job produced 2 rows", nil
	})
}
