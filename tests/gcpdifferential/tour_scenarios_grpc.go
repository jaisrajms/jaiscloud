//go:build gcp_differential

package gcpdifferential

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The gRPC half of the SDK-tour differential. It drives the official
// proto-level clients for the services the tour uses over gRPC (KMS, Secret
// Manager, Firestore, Logging, Pub/Sub including the IAM policy surface) — the
// same clients the tour's Go implementation wires to the emulator — and records
// the exact request/response protos into testdata/golden-tour-grpc.
//
// Streaming surfaces the tour exercises (Pub/Sub streaming pull, Firestore
// Listen, Logging TailLogEntries) and the Dataproc LRO create are documented
// deferrals: see tour_scenarios_grpc.go's package docs and the tour README.

// TourGRPCScenarios returns the gRPC SDK-tour scenario list.
func TourGRPCScenarios(project, suffix string) []GRPCScenario {
	n := Names(suffix)

	var sc []GRPCScenario

	// ─── Cloud KMS: symmetric round-trip + asymmetric sign/verify ────────────
	// The keys are fixed, reusable (non-deletable) resources created by
	// EnsureTourKMS, so a recording leaves no residue.
	kmsBase := "projects/" + project + "/locations/global/keyRings/" + FixedKMSKeyRing + "/cryptoKeys/"
	symKey := kmsBase + FixedKMSCryptoKey
	asymVersion := kmsBase + FixedKMSCryptoKeyAsym + "/cryptoKeyVersions/1"

	sc = append(sc,
		GRPCScenario{
			Service: "kms", Op: "tour_kms_encrypt",
			Method: "google.cloud.kms.v1.KeyManagementService/Encrypt", Path: symKey,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &kmspb.EncryptRequest{Name: symKey, Plaintext: []byte("sdk-tour")}
				var resp *kmspb.EncryptResponse
				err := withKMS(ctx, t, func(c kmspb.KeyManagementServiceClient) error {
					var cerr error
					resp, cerr = c.Encrypt(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "kms", Op: "tour_kms_decrypt",
			Method: "google.cloud.kms.v1.KeyManagementService/Decrypt", Path: symKey,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				// Encrypt live so the round-trip is self-contained; the request
				// ciphertext folds, so record/replay goldens stay stable.
				var ct []byte
				err := withKMS(ctx, t, func(c kmspb.KeyManagementServiceClient) error {
					enc, cerr := c.Encrypt(ctx, &kmspb.EncryptRequest{Name: symKey, Plaintext: []byte("sdk-tour")})
					if cerr != nil {
						return cerr
					}
					ct = enc.GetCiphertext()
					return nil
				})
				if err != nil {
					return nil, nil, err
				}
				req := &kmspb.DecryptRequest{Name: symKey, Ciphertext: ct}
				var resp *kmspb.DecryptResponse
				err = withKMS(ctx, t, func(c kmspb.KeyManagementServiceClient) error {
					var cerr error
					resp, cerr = c.Decrypt(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "kms", Op: "tour_kms_asym_get_public_key",
			Method: "google.cloud.kms.v1.KeyManagementService/GetPublicKey", Path: asymVersion,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &kmspb.GetPublicKeyRequest{Name: asymVersion}
				var resp *kmspb.PublicKey
				err := withKMS(ctx, t, func(c kmspb.KeyManagementServiceClient) error {
					var cerr error
					resp, cerr = c.GetPublicKey(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "kms", Op: "tour_kms_asym_sign",
			Method: "google.cloud.kms.v1.KeyManagementService/AsymmetricSign", Path: asymVersion,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				digest := sha256.Sum256([]byte("sdk-tour-sign"))
				req := &kmspb.AsymmetricSignRequest{
					Name:   asymVersion,
					Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
				}
				var resp *kmspb.AsymmetricSignResponse
				err := withKMS(ctx, t, func(c kmspb.KeyManagementServiceClient) error {
					var cerr error
					resp, cerr = c.AsymmetricSign(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// ─── Secret Manager: add / access / list ─────────────────────────────────
	secretParent := "projects/" + project
	secretName := secretParent + "/secrets/" + n.TourSecret
	sc = append(sc,
		GRPCScenario{
			Service: "secretmanager", Op: "tour_secret_create",
			Method: "google.cloud.secretmanager.v1.SecretManagerService/CreateSecret", Path: secretName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &secretmanagerpb.CreateSecretRequest{
					Parent:   secretParent,
					SecretId: n.TourSecret,
					Secret: &secretmanagerpb.Secret{
						Replication: &secretmanagerpb.Replication{
							Replication: &secretmanagerpb.Replication_Automatic_{
								Automatic: &secretmanagerpb.Replication_Automatic{},
							},
						},
					},
				}
				var resp *secretmanagerpb.Secret
				err := withSecretManager(ctx, t, func(c secretmanagerpb.SecretManagerServiceClient) error {
					var cerr error
					resp, cerr = c.CreateSecret(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "secretmanager", Op: "tour_secret_add_version",
			Method: "google.cloud.secretmanager.v1.SecretManagerService/AddSecretVersion", Path: secretName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &secretmanagerpb.AddSecretVersionRequest{
					Parent:  secretName,
					Payload: &secretmanagerpb.SecretPayload{Data: []byte("sdk-tour")},
				}
				var resp *secretmanagerpb.SecretVersion
				err := withSecretManager(ctx, t, func(c secretmanagerpb.SecretManagerServiceClient) error {
					var cerr error
					resp, cerr = c.AddSecretVersion(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "secretmanager", Op: "tour_secret_access",
			Method: "google.cloud.secretmanager.v1.SecretManagerService/AccessSecretVersion", Path: secretName + "/versions/1",
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &secretmanagerpb.AccessSecretVersionRequest{Name: secretName + "/versions/1"}
				var resp *secretmanagerpb.AccessSecretVersionResponse
				err := withSecretManager(ctx, t, func(c secretmanagerpb.SecretManagerServiceClient) error {
					var cerr error
					resp, cerr = c.AccessSecretVersion(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "secretmanager", Op: "tour_secret_list_versions",
			Method: "google.cloud.secretmanager.v1.SecretManagerService/ListSecretVersions", Path: secretName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &secretmanagerpb.ListSecretVersionsRequest{Parent: secretName}
				var resp *secretmanagerpb.ListSecretVersionsResponse
				err := withSecretManager(ctx, t, func(c secretmanagerpb.SecretManagerServiceClient) error {
					var cerr error
					resp, cerr = c.ListSecretVersions(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// ─── Firestore: transaction read-modify-write + cursor pagination ───────
	fsRoot := "projects/" + project + "/databases/(default)/documents"
	// The database resource name (no "/documents" suffix) is what Commit and
	// BeginTransaction address; passing the documents root is INVALID_ARGUMENT.
	fsDB := "projects/" + project + "/databases/(default)"
	txDoc := fsRoot + "/" + n.TourFSCounter + "/counter"
	txCollection := fsRoot + "/" + n.TourFSCounter
	pageCollection := fsRoot + "/" + n.TourFSPage

	intValue := func(v int64) *firestorepb.Value {
		return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
	}

	sc = append(sc,
		GRPCScenario{
			Service: "firestore", Op: "tour_fs_tx_seed",
			Method: "google.firestore.v1.Firestore/Commit", Path: txCollection,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.CommitRequest{
					Database: fsDB,
					Writes: []*firestorepb.Write{{
						Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
							Name:   txDoc,
							Fields: map[string]*firestorepb.Value{"n": intValue(0)},
						}},
					}},
				}
				var resp *firestorepb.CommitResponse
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.Commit(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "firestore", Op: "tour_fs_tx_commit",
			Method: "google.firestore.v1.Firestore/Commit", Path: txDoc,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				var req *firestorepb.CommitRequest
				var resp *firestorepb.CommitResponse
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					begin, cerr := c.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: fsDB})
					if cerr != nil {
						return cerr
					}
					// Read the current value inside the transaction, then write
					// back value+1 (the tour's read-modify-write).
					cur, cerr := c.GetDocument(ctx, &firestorepb.GetDocumentRequest{
						Name:                txDoc,
						ConsistencySelector: &firestorepb.GetDocumentRequest_Transaction{Transaction: begin.GetTransaction()},
					})
					if cerr != nil {
						return cerr
					}
					next := cur.GetFields()["n"].GetIntegerValue() + 1
					req = &firestorepb.CommitRequest{
						Database:    fsDB,
						Transaction: begin.GetTransaction(),
						Writes: []*firestorepb.Write{{
							Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
								Name:   txDoc,
								Fields: map[string]*firestorepb.Value{"n": intValue(next)},
							}},
						}},
					}
					resp, cerr = c.Commit(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// Pagination fixture: five documents (i = 0..4) written in one Commit, then
	// three ordered pages of two using explicit cursor values. The cursor keeps
	// the pages independent (no run-to-run chaining), while still exercising the
	// StartAt-after semantics the SDK's StartAfter produces.
	pageWrites := make([]*firestorepb.Write, 0, 5)
	for i := 0; i < 5; i++ {
		pageWrites = append(pageWrites, &firestorepb.Write{
			Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
				Name:   fmt.Sprintf("%s/d%d", pageCollection, i),
				Fields: map[string]*firestorepb.Value{"i": intValue(int64(i))},
			}},
		})
	}
	runQueryPage := func(cursor *int64) func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
		return func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
			q := &firestorepb.StructuredQuery{
				From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: n.TourFSPage}},
				OrderBy: []*firestorepb.StructuredQuery_Order{{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "i"}, Direction: firestorepb.StructuredQuery_ASCENDING}},
				Limit:   wrapperspb.Int32(2),
			}
			if cursor != nil {
				// Before=false is "start just after the value", i.e. the SDK's
				// StartAfter.
				q.StartAt = &firestorepb.Cursor{
					Values: []*firestorepb.Value{intValue(*cursor)},
					Before: false,
				}
			}
			req := &firestorepb.RunQueryRequest{
				Parent:    fsRoot,
				QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: q},
			}
			frames, err := runQueryFrames(ctx, t, req)
			if err != nil {
				return req, nil, err
			}
			return req, frames, nil
		}
	}

	sc = append(sc,
		GRPCScenario{
			Service: "firestore", Op: "tour_fs_page_seed",
			Method: "google.firestore.v1.Firestore/Commit", Path: pageCollection,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.CommitRequest{Database: fsDB, Writes: pageWrites}
				var resp *firestorepb.CommitResponse
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.Commit(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{Service: "firestore", Op: "tour_fs_query_page_1",
			Method: "google.firestore.v1.Firestore/RunQuery", Path: pageCollection,
			Call: runQueryPage(nil)},
		GRPCScenario{Service: "firestore", Op: "tour_fs_query_page_2",
			Method: "google.firestore.v1.Firestore/RunQuery", Path: pageCollection,
			Call: runQueryPage(int64Ptr(1))},
		GRPCScenario{Service: "firestore", Op: "tour_fs_query_page_3",
			Method: "google.firestore.v1.Firestore/RunQuery", Path: pageCollection,
			Call: runQueryPage(int64Ptr(3))},
	)

	// ─── Cloud Logging: write + list ─────────────────────────────────────────
	logName := "projects/" + project + "/logs/" + n.TourLog
	sc = append(sc,
		GRPCScenario{
			Service: "logging", Op: "tour_log_write",
			Method: "google.logging.v2.LoggingServiceV2/WriteLogEntries", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &loggingpb.WriteLogEntriesRequest{
					LogName: logName,
					Resource: &monitoredres.MonitoredResource{
						Type:   "global",
						Labels: map[string]string{"project_id": project},
					},
					Entries: []*loggingpb.LogEntry{{
						LogName:  logName,
						Severity: ltype.LogSeverity_INFO,
						Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: "sdk-tour-entry"},
					}},
				}
				var resp *loggingpb.WriteLogEntriesResponse
				err := withLogging(ctx, t, func(c loggingpb.LoggingServiceV2Client) error {
					var cerr error
					resp, cerr = c.WriteLogEntries(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "logging", Op: "tour_log_list",
			Method: "google.logging.v2.LoggingServiceV2/ListLogEntries", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &loggingpb.ListLogEntriesRequest{
					ResourceNames: []string{"projects/" + project},
					Filter:        "logName=\"" + logName + "\"",
					PageSize:      10,
				}
				var resp *loggingpb.ListLogEntriesResponse
				err := withLogging(ctx, t, func(c loggingpb.LoggingServiceV2Client) error {
					var cerr error
					resp, cerr = c.ListLogEntries(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
			Wait: &GRPCWait{Until: func(m proto.Message) bool {
				r, ok := m.(*loggingpb.ListLogEntriesResponse)
				return ok && len(r.GetEntries()) > 0
			}},
		},
	)

	// ─── Pub/Sub: topic, batched Publish, and the IAM policy surface ────────
	topicName := "projects/" + project + "/topics/" + n.TourTopic
	sc = append(sc,
		GRPCScenario{
			Service: "pubsub", Op: "tour_pubsub_topic_create",
			Method: "google.pubsub.v1.Publisher/CreateTopic", Path: topicName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &pubsubpb.Topic{Name: topicName}
				var resp *pubsubpb.Topic
				err := withPubsubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
					var cerr error
					resp, cerr = c.CreateTopic(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "pubsub", Op: "tour_pubsub_publish",
			Method: "google.pubsub.v1.Publisher/Publish", Path: topicName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				// The SDK batches; the wire op is one Publish with N messages.
				msgs := make([]*pubsubpb.PubsubMessage, 0, 4)
				for i := 0; i < 4; i++ {
					msgs = append(msgs, &pubsubpb.PubsubMessage{Data: []byte(fmt.Sprintf("batch-%02d", i))})
				}
				req := &pubsubpb.PublishRequest{Topic: topicName, Messages: msgs}
				var resp *pubsubpb.PublishResponse
				err := withPubsubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
					var cerr error
					resp, cerr = c.Publish(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "pubsub", Op: "tour_pubsub_iam_get",
			Method: "google.iam.v1.IAMPolicy/GetIamPolicy", Path: topicName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &iampb.GetIamPolicyRequest{
					Resource: topicName,
					Options:  &iampb.GetPolicyOptions{RequestedPolicyVersion: 3},
				}
				var resp *iampb.Policy
				err := withPubsubIAM(ctx, t, func(c iampb.IAMPolicyClient) error {
					var cerr error
					resp, cerr = c.GetIamPolicy(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "pubsub", Op: "tour_pubsub_iam_set",
			Method: "google.iam.v1.IAMPolicy/SetIamPolicy", Path: topicName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &iampb.SetIamPolicyRequest{
					Resource: topicName,
					Policy: &iampb.Policy{
						Bindings: []*iampb.Binding{{Role: "roles/pubsub.publisher", Members: []string{"allUsers"}}},
					},
				}
				var resp *iampb.Policy
				err := withPubsubIAM(ctx, t, func(c iampb.IAMPolicyClient) error {
					var cerr error
					resp, cerr = c.SetIamPolicy(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	return sc
}

// runQueryFrames drains a Firestore RunQuery server stream and returns the
// document frames as a synthetic proto whose JSON shape is {"frames": [...]}.
// A server-streaming RPC yields one response per document (plus a terminal
// frame), so the frames — not a single response — are the observable.
func runQueryFrames(ctx context.Context, t *GRPCTarget, req *firestorepb.RunQueryRequest) (*structpb.Struct, error) {
	conn, err := t.dial(ctx, "firestore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stream, err := firestorepb.NewFirestoreClient(conn).RunQuery(ctx, req)
	if err != nil {
		return nil, err
	}
	var frames []any
	for {
		resp, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
		if resp.GetDocument() == nil {
			continue
		}
		b, merr := marshalProtoJSON(resp)
		if merr != nil {
			return nil, merr
		}
		var m map[string]any
		if uerr := json.Unmarshal(b, &m); uerr != nil {
			return nil, uerr
		}
		frames = append(frames, m)
	}
	if frames == nil {
		frames = []any{}
	}
	return structpb.NewStruct(map[string]any{"frames": frames})
}

func int64Ptr(v int64) *int64 { return &v }

// withKMS dials the target's KMS endpoint and runs fn with the generated
// KeyManagementService stub.
func withKMS(ctx context.Context, t *GRPCTarget, fn func(kmspb.KeyManagementServiceClient) error) error {
	conn, err := t.dial(ctx, "kms")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(kmspb.NewKeyManagementServiceClient(conn))
}

// withSecretManager dials the target's Secret Manager endpoint and runs fn with
// the generated SecretManagerService stub.
func withSecretManager(ctx context.Context, t *GRPCTarget, fn func(secretmanagerpb.SecretManagerServiceClient) error) error {
	conn, err := t.dial(ctx, "secretmanager")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(secretmanagerpb.NewSecretManagerServiceClient(conn))
}

// withPubsubPublisher dials the target's Pub/Sub endpoint and runs fn with the
// generated Publisher stub.
func withPubsubPublisher(ctx context.Context, t *GRPCTarget, fn func(pubsubpb.PublisherClient) error) error {
	conn, err := t.dial(ctx, "pubsub")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(pubsubpb.NewPublisherClient(conn))
}

// withPubsubIAM dials the Pub/Sub endpoint and runs fn with the
// google.iam.v1.IAMPolicy stub that Pub/Sub hosts.
func withPubsubIAM(ctx context.Context, t *GRPCTarget, fn func(iampb.IAMPolicyClient) error) error {
	conn, err := t.dial(ctx, "pubsub")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(iampb.NewIAMPolicyClient(conn))
}

// CleanupTourGRPC deletes the resources the gRPC tour set creates (the secret,
// the Firestore documents and the Pub/Sub topic). KMS resources are excluded
// (non-deletable) and log entries expire on their own.
func (t *GRPCTarget) CleanupTourGRPC(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	n := t.Names
	var log []string

	// Secret.
	err := withSecretManager(ctx, t, func(c secretmanagerpb.SecretManagerServiceClient) error {
		_, cerr := c.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{
			Name: "projects/" + t.Project + "/secrets/" + n.TourSecret,
		})
		return cerr
	})
	if err != nil {
		log = append(log, "cleanup tour secret: "+err.Error())
	} else {
		log = append(log, "cleanup tour secret: ok")
	}

	// Firestore: the transaction counter doc and the five pagination docs.
	fsRoot := "projects/" + t.Project + "/databases/(default)/documents"
	docNames := []string{fsRoot + "/" + n.TourFSCounter + "/counter"}
	for i := 0; i < 5; i++ {
		docNames = append(docNames, fmt.Sprintf("%s/%s/d%d", fsRoot, n.TourFSPage, i))
	}
	// Delete all documents over a single connection (dialing per document is
	// slow against real GCP). A delete racing a just-committed transaction
	// answers ABORTED; retry it.
	ferr := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
		for _, name := range docNames {
			var derr error
			for attempt := 0; attempt < 3; attempt++ {
				_, derr = c.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name})
				if derr == nil || status.Code(derr) != codes.Aborted {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			if derr != nil && status.Code(derr) != codes.NotFound {
				log = append(log, "cleanup tour firestore "+name+": "+derr.Error())
			}
		}
		return nil
	})
	if ferr != nil {
		log = append(log, "cleanup tour firestore connection: "+ferr.Error())
	}
	log = append(log, "cleanup tour firestore documents: done")

	// Pub/Sub topic.
	err = withPubsubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
		_, cerr := c.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: "projects/" + t.Project + "/topics/" + n.TourTopic})
		return cerr
	})
	if err != nil {
		log = append(log, "cleanup tour pubsub topic: "+err.Error())
	} else {
		log = append(log, "cleanup tour pubsub topic: ok")
	}

	return log
}
