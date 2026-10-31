//go:build gcp_conformance

package gcpconformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/parquet-go/parquet-go"
)

// defaultProject is the emulator's default GCP project (internal/config).
const defaultProject = "jaiscloud-project"

// Scenario is one curated request in the record sequence. Body and Path may
// reference ${name} variables captured from earlier responses via Save.
type Scenario struct {
	Service     string
	Method      string
	Path        string
	Body        string
	ContentType string
	// Host, when set, overrides the request's Host header. It is used for the
	// services that disambiguate a shared canonical path by host token: GKE's
	// container.* host, and the Managed Kafka / Dataproc Metastore operations
	// family that shares locations/{location}/operations with Cloud Workflows.
	Host string
	// Save maps a variable name to a dotted path into the response JSON whose
	// scalar value is captured for use by later scenarios (e.g. the ciphertext
	// returned by KMS encrypt feeding KMS decrypt).
	Save map[string]string
	// Prepare, when set, builds the request body from previously saved
	// variables (Body is ignored). It models client-side work a real caller
	// performs between two API calls — wrapping key material with an import
	// job's public key, sealing with an asymmetric public key, encapsulating
	// with a KEM public key. The bytes it returns are sent to the public API
	// unchanged, so no emulator behaviour is bypassed.
	Prepare func(vars map[string]string) (string, error)
	// AdvanceClock, when set, reconfigures the emulator's clock through the
	// admin control plane before the request is issued; the control-plane call
	// is not part of the recorded transcript. "real" restores wall time, and a
	// Go duration ("744h") offsets the clock from real time. State machines
	// that only advance on an elapsed window (a scheduled key destruction)
	// need it; the request under test is still the public API.
	AdvanceClock string
}

// runSuffix returns a per-run unique suffix so scenario names stay idempotent
// across repeated record runs against the same emulator.
func runSuffix() string {
	return fmt.Sprintf("%06x%06x", os.Getpid()&0xffffff, time.Now().UnixNano()&0xffffff)
}

// Scenarios returns the curated, idempotent request list covering storage,
// pubsub, secretmanager, kms, clouddns, bigquery and iam, including both
// success and error (404) responses.
func Scenarios(suffix string) []Scenario {
	p := defaultProject

	bucket := "conf-bucket-" + suffix
	topic := "conf-topic-" + suffix
	sub := "conf-sub-" + suffix
	secret := "conf-secret-" + suffix
	ring := "conf-ring-" + suffix
	key := "conf-key-" + suffix
	sa := "conf-sa-" + suffix
	zone := "conf-zone-" + suffix
	dnsName := "conf" + suffix + ".example.com."
	ds := "conf_ds_" + suffix
	tbl := "conf_tbl_" + suffix

	saEmail := sa + "@" + p + ".iam.gserviceaccount.com"
	topicName := "projects/" + p + "/topics/" + topic
	subName := "projects/" + p + "/subscriptions/" + sub

	var sc []Scenario

	// ─── Cloud Storage (GCS JSON API) ─────────────────────────────────────────
	sc = append(sc,
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q}`, bucket)},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b?project=" + p},
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=hello.txt",
			Body:        "hello jaiscloud",
			ContentType: "text/plain"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket + "/o/hello.txt"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket + "/o"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket + "/o/missing-" + suffix},
		// A delete soft-deletes the generation (the bucket's default 7-day
		// policy); the bucket itself can still be deleted because it holds only
		// soft-deleted objects.
		Scenario{Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + bucket + "/o/hello.txt"},
		Scenario{Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + bucket},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket},
	)

	// ─── Cloud Storage: bucket admin, ACL/IAM, object verbs, notifications ────
	// These operations cannot be staged by the generic Discovery probe: they
	// need a live bucket, object generations, a locked retention policy and a
	// Pub/Sub topic. The chain drives the public JSON API only and captures the
	// server-assigned values (metageneration, generation, notification id, IAM
	// etag) from earlier responses.
	sdBucket := "conf-sd-bucket-" + suffix
	lockBucket := "conf-lock-bucket-" + suffix
	notifTopic := "conf-notif-topic-" + suffix
	notifTopicName := "projects/" + p + "/topics/" + notifTopic
	sc = append(sc,
		// Unversioned bucket: deletes are soft deletes (7-day default policy),
		// which is what objects.restore accepts.
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q}`, sdBucket)},
		// buckets.update is a full replace (PUT), not a merge.
		Scenario{Service: "storage", Method: "PUT", Path: "/storage/v1/b/" + sdBucket,
			Body: fmt.Sprintf(`{"name":%q,"storageClass":"STANDARD","labels":{"env":"conformance"}}`, sdBucket)},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/storageLayout"},
		// Bucket IAM: get the policy (capturing its etag for OCC), then set it.
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/iam",
			Save: map[string]string{"smBucketIamEtag": "etag"}},
		Scenario{Service: "storage", Method: "PUT", Path: "/storage/v1/b/" + sdBucket + "/iam",
			Body: `{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}],"etag":"${smBucketIamEtag}","version":3}`},
		// Bucket ACLs (legacy access-control view).
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/acl"},
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b/" + sdBucket + "/acl",
			Body: `{"entity":"allUsers","role":"READER"}`},
		// A separate bucket carries the (irreversible) retention lock: locking
		// makes its objects undeletable, so it never hosts the object chain.
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q,"retentionPolicy":{"retentionPeriod":"10"}}`, lockBucket),
			Save: map[string]string{"smLockMeta": "metageneration"}},
		Scenario{Service: "storage", Method: "POST",
			Path: "/storage/v1/b/" + lockBucket + "/lockRetentionPolicy?ifMetagenerationMatch=${smLockMeta}"},
		// Objects: media uploads give the generations the verbs below address.
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + sdBucket + "/o?uploadType=media&name=o1.txt",
			Body:        "alpha",
			ContentType: "text/plain"},
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + sdBucket + "/o?uploadType=media&name=o2.txt",
			Body:        "beta",
			ContentType: "text/plain"},
		// objects.patch merges; objects.update replaces.
		Scenario{Service: "storage", Method: "PATCH", Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt",
			Body: `{"metadata":{"stage":"patched"}}`},
		Scenario{Service: "storage", Method: "PUT", Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt",
			Body: `{"name":"o1.txt","contentType":"text/plain","metadata":{"stage":"replaced"}}`},
		Scenario{Service: "storage", Method: "POST",
			Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt/copyTo/b/" + sdBucket + "/o/c1.txt",
			Body: `{"name":"c1.txt"}`},
		Scenario{Service: "storage", Method: "POST",
			Path: "/storage/v1/b/" + sdBucket + "/o/c1.txt/rewriteTo/b/" + sdBucket + "/o/r1.txt",
			Body: `{"name":"r1.txt"}`},
		Scenario{Service: "storage", Method: "POST",
			Path: "/storage/v1/b/" + sdBucket + "/o/comp.txt/compose",
			Body: `{"sourceObjects":[{"name":"o1.txt"},{"name":"o2.txt"}],"destination":{"contentType":"text/plain"}}`},
		Scenario{Service: "storage", Method: "POST",
			Path: "/storage/v1/b/" + sdBucket + "/o/comp.txt/moveTo/o/moved.txt"},
		// Object-level ACL and IAM (the legacy ACL view and the IAM view).
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt/acl"},
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt/acl",
			Body: `{"entity":"allUsers","role":"READER"}`},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt/iam",
			Save: map[string]string{"smObjectIamEtag": "etag"}},
		Scenario{Service: "storage", Method: "PUT", Path: "/storage/v1/b/" + sdBucket + "/o/o1.txt/iam",
			Body: `{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}],"etag":"${smObjectIamEtag}","version":3}`},
		// Notifications need a Pub/Sub topic; capture the assigned id for
		// get/list/delete.
		Scenario{Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + p + "/topics/" + notifTopic,
			Body: fmt.Sprintf(`{"name":%q}`, notifTopicName)},
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b/" + sdBucket + "/notificationConfigs",
			Body: fmt.Sprintf(`{"topic":%q,"payload_format":"JSON_API_V1","event_types":["OBJECT_FINALIZE"]}`, notifTopicName),
			Save: map[string]string{"smNotifID": "id"}},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/notificationConfigs/${smNotifID}"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + sdBucket + "/notificationConfigs"},
		Scenario{Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + sdBucket + "/notificationConfigs/${smNotifID}"},
		// objects.restore: soft-delete a generation (the default bucket policy),
		// then restore it. Real GCS answers with a new-generation object copy.
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + sdBucket + "/o?uploadType=media&name=soft.txt",
			Body:        "restorable",
			ContentType: "text/plain",
			Save:        map[string]string{"smSoftGen": "generation"}},
		Scenario{Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + sdBucket + "/o/soft.txt"},
		Scenario{Service: "storage", Method: "POST",
			Path: "/storage/v1/b/" + sdBucket + "/o/soft.txt/restore?generation=${smSoftGen}"},
	)

	// ─── Pub/Sub ──────────────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + p + "/topics/" + topic,
			Body: fmt.Sprintf(`{"name":%q}`, topicName)},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/topics/" + topic},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/topics"},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/topics/missing-" + suffix},
		Scenario{Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + p + "/subscriptions/" + sub,
			Body: fmt.Sprintf(`{"name":%q,"topic":%q,"ackDeadlineSeconds":10}`, subName, topicName)},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/subscriptions/" + sub},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/subscriptions"},
		Scenario{Service: "pubsub", Method: "POST", Path: "/v1/projects/" + p + "/topics/" + topic + ":publish",
			Body: `{"messages":[{"data":"aGVsbG8="}]}`},
		Scenario{Service: "pubsub", Method: "POST", Path: "/v1/projects/" + p + "/subscriptions/" + sub + ":pull",
			Body: `{"maxMessages":10}`},
		Scenario{Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + p + "/subscriptions/" + sub},
		Scenario{Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + p + "/topics/" + topic},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/subscriptions/missing-" + suffix},
	)

	// ─── Secret Manager ───────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + p + "/secrets?secretId=" + secret,
			Body: `{"replication":{"automatic":{}}}`},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets/" + secret},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets"},
		Scenario{Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + p + "/secrets/" + secret + ":addVersion",
			Body: `{"payload":{"data":"c2VjcmV0"}}`},
		Scenario{Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + p + "/secrets/" + secret + "/versions/1:access"},
		Scenario{Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + p + "/secrets/" + secret + "/versions"},
		Scenario{Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + p + "/secrets/" + secret + ":enableManagedRotation",
			Body: `{"cloudSqlSingleUserCredentials":{"instanceId":"conf-instance","username":"conf-user"}}`},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets/" + secret},
		Scenario{Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + p + "/secrets/" + secret + ":rotateSecret", Body: `{}`},
		Scenario{Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + p + "/secrets/" + secret + "/versions"},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets/missing-" + suffix},
		Scenario{Service: "secretmanager", Method: "DELETE", Path: "/v1/projects/" + p + "/secrets/" + secret},
	)

	// ─── Cloud KMS ────────────────────────────────────────────────────────────
	base := "/v1/projects/" + p + "/locations/global/keyRings"
	keyPath := base + "/" + ring + "/cryptoKeys/" + key
	sc = append(sc,
		Scenario{Service: "kms", Method: "POST", Path: base + "?keyRingId=" + ring, Body: `{}`},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring},
		Scenario{Service: "kms", Method: "GET", Path: base},
		Scenario{Service: "kms", Method: "POST", Path: base + "/" + ring + "/cryptoKeys?cryptoKeyId=" + key,
			Body: `{"purpose":"ENCRYPT_DECRYPT"}`, Save: map[string]string{"cryptoKey": "name"}},
		Scenario{Service: "kms", Method: "GET", Path: keyPath},
		Scenario{Service: "kms", Method: "PATCH", Path: keyPath + "?updateMask=labels",
			Body: `{"labels":{"env":"conf"}}`},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring + "/cryptoKeys"},
		Scenario{Service: "kms", Method: "POST", Path: keyPath + ":encrypt",
			Body: `{"plaintext":"aGVsbG8="}`, Save: map[string]string{"ciphertext": "ciphertext"}},
		Scenario{Service: "kms", Method: "POST", Path: keyPath + ":decrypt",
			Body: `{"ciphertext":"${ciphertext}"}`},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring + ":getIamPolicy"},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring + "/cryptoKeys/missing-" + suffix},
	)

	// ─── Cloud KMS: key/version lifecycle evidence ────────────────────────────
	// The 18 crypto-key and crypto-key-version cells above have no validated
	// response from the generic probe: it cannot stage an import job's wrapping
	// key, an HSM trusted-wrapping key, a KEM public key, or an elapsed
	// destruction window. This chain drives the public JSON API only and
	// captures the server-assigned names, IVs, MACs and import-job keys that
	// later calls address. The client-side cryptography a real caller performs
	// between calls (RSA-OAEP wrapping, AES-KWP, ML-KEM encapsulation, RSAES-OAEP
	// sealing) lives in Prepare/clientcrypto.go.
	kmsRing := "conf-kms-" + suffix
	kmsBase := "/v1/projects/" + p + "/locations/global/keyRings/" + kmsRing
	kmsName := "projects/" + p + "/locations/global/keyRings/" + kmsRing
	symMaterial := []byte("conformance-symmetric-key-000001")
	kwpMaterial := bytes.Repeat([]byte{0x4b}, 32)
	trustedMaterial := []byte("trusted-material-conformance-001")
	trustedWrapped, err := aesKeyWrapWithPadding(kwpMaterial, trustedMaterial)
	if err != nil {
		panic("kms trusted-wrap fixture: " + err.Error())
	}
	signDigest := sha256.Sum256([]byte("conformance digest"))
	sc = append(sc,
		Scenario{Service: "kms", Method: "POST",
			Path: "/v1/projects/" + p + "/locations/global/keyRings?keyRingId=" + kmsRing, Body: `{}`},

		// Symmetric key: a second version, primary selection, and the
		// enable/disable state machine (cryptoKeyVersions.patch).
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=sym",
			Body: `{"purpose":"ENCRYPT_DECRYPT"}`},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/sym/cryptoKeyVersions", Body: `{}`},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/sym:updatePrimaryVersion",
			Body: `{"cryptoKeyVersionId":"2"}`},
		Scenario{Service: "kms", Method: "GET", Path: kmsBase + "/cryptoKeys/sym/cryptoKeyVersions/2"},
		Scenario{Service: "kms", Method: "PATCH",
			Path: kmsBase + "/cryptoKeys/sym/cryptoKeyVersions/2?updateMask=state", Body: `{"state":"DISABLED"}`},
		Scenario{Service: "kms", Method: "PATCH",
			Path: kmsBase + "/cryptoKeys/sym/cryptoKeyVersions/2?updateMask=state", Body: `{"state":"ENABLED"}`},
		Scenario{Service: "kms", Method: "POST",
			Path: kmsBase + "/cryptoKeys/sym/cryptoKeyVersions/2:destroy", Body: `{}`},
		Scenario{Service: "kms", Method: "POST",
			Path: kmsBase + "/cryptoKeys/sym/cryptoKeyVersions/2:restore", Body: `{}`},

		// Raw AES-GCM: the server-generated IV from rawEncrypt feeds rawDecrypt.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=raw",
			Body: `{"purpose":"RAW_ENCRYPT_DECRYPT"}`},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/raw/cryptoKeyVersions/1:rawEncrypt",
			Body: fmt.Sprintf(`{"plaintext":%q}`, b64([]byte("raw-encrypt-plaintext"))),
			Save: map[string]string{"kmsRawCT": "ciphertext", "kmsRawIV": "initializationVector"}},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/raw/cryptoKeyVersions/1:rawDecrypt",
			Body: `{"ciphertext":"${kmsRawCT}","initializationVector":"${kmsRawIV}","tagLength":16}`},

		// HMAC: macSign's output verifies through macVerify.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=mac",
			Body: `{"purpose":"MAC","versionTemplate":{"algorithm":"HMAC_SHA256"}}`},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/mac/cryptoKeyVersions/1:macSign",
			Body: fmt.Sprintf(`{"data":%q}`, b64([]byte("mac-conformance-data"))),
			Save: map[string]string{"kmsMac": "mac"}},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/mac/cryptoKeyVersions/1:macVerify",
			Body: fmt.Sprintf(`{"data":%q,"mac":"${kmsMac}"}`, b64([]byte("mac-conformance-data")))},

		// Asymmetric signing (ECDSA P-256): the public key and a SHA-256 digest.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=sign",
			Body: `{"purpose":"ASYMMETRIC_SIGN","versionTemplate":{"algorithm":"EC_SIGN_P256_SHA256"}}`},
		Scenario{Service: "kms", Method: "GET", Path: kmsBase + "/cryptoKeys/sign/cryptoKeyVersions/1/publicKey"},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/sign/cryptoKeyVersions/1:asymmetricSign",
			Body: fmt.Sprintf(`{"digest":{"sha256":%q}}`, b64(signDigest[:]))},

		// Asymmetric decryption (RSA-OAEP-2048): seal with the public key.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=dec",
			Body: `{"purpose":"ASYMMETRIC_DECRYPT","versionTemplate":{"algorithm":"RSA_DECRYPT_OAEP_2048_SHA256"}}`},
		Scenario{Service: "kms", Method: "GET", Path: kmsBase + "/cryptoKeys/dec/cryptoKeyVersions/1/publicKey",
			Save: map[string]string{"kmsDecPub": "pem"}},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/dec/cryptoKeyVersions/1:asymmetricDecrypt",
			Prepare: func(vars map[string]string) (string, error) {
				sealed, err := rsaOAEPEncrypt(vars["kmsDecPub"], []byte("asymmetric-plaintext"))
				if err != nil {
					return "", err
				}
				return fmt.Sprintf(`{"ciphertext":%q}`, b64(sealed)), nil
			}},

		// Key encapsulation (ML-KEM-768): encapsulate against the public key.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=kem",
			Body: `{"purpose":"KEY_ENCAPSULATION","versionTemplate":{"algorithm":"ML_KEM_768"}}`},
		// PQC algorithms require public_key_format (Cloud KMS rejects the call
		// without it); the response carries the key as ChecksummedData.
		Scenario{Service: "kms", Method: "GET",
			Path: kmsBase + "/cryptoKeys/kem/cryptoKeyVersions/1/publicKey?publicKeyFormat=NIST_PQC",
			Save: map[string]string{"kmsKemPub": "publicKey.data"}},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/kem/cryptoKeyVersions/1:decapsulate",
			Prepare: func(vars map[string]string) (string, error) {
				raw, err := base64.StdEncoding.DecodeString(vars["kmsKemPub"])
				if err != nil {
					return "", err
				}
				ciphertext, err := mlkemEncapsulate(raw)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf(`{"ciphertext":%q}`, b64(ciphertext)), nil
			}},

		// Import: the job exposes a wrapping public key; the material is wrapped
		// with RSA-OAEP + AES-KWP and imported as the first version of an
		// import-only key (real Cloud KMS only accepts imports into such keys).
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=imp&skipInitialVersionCreation=true",
			Body: `{"purpose":"ENCRYPT_DECRYPT","importOnly":true}`},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/importJobs?importJobId=job-soft",
			Body: `{"importMethod":"RSA_OAEP_3072_SHA256_AES_256","protectionLevel":"SOFTWARE"}`,
			Save: map[string]string{"kmsJobPub": "publicKey.pem"}},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/imp/cryptoKeyVersions:import",
			Prepare: func(vars map[string]string) (string, error) {
				wrapped, err := wrapImportMaterial(vars["kmsJobPub"], symMaterial)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf(`{"importJob":%q,"algorithm":"GOOGLE_SYMMETRIC_ENCRYPTION","wrappedKey":%q}`,
					kmsName+"/importJobs/job-soft", b64(wrapped)), nil
			}},

		// Trusted wrapping: an HSM AES-256-KWP importing key is itself imported
		// (so its material is known client-side), then used to unwrap a
		// trusted-wrapped version and to export it again.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/importJobs?importJobId=job-hsm",
			Body: `{"importMethod":"RSA_OAEP_3072_SHA256_AES_256","protectionLevel":"HSM_SINGLE_TENANT"}`,
			Save: map[string]string{"kmsHsmPub": "publicKey.pem"}},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=kwp&skipInitialVersionCreation=true",
			Body: `{"purpose":"AES_WRAPPING","importOnly":true,"versionTemplate":{"algorithm":"AES_256_KWP","protectionLevel":"HSM_SINGLE_TENANT"}}`},
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys/kwp/cryptoKeyVersions:import",
			Prepare: func(vars map[string]string) (string, error) {
				wrapped, err := wrapImportMaterial(vars["kmsHsmPub"], kwpMaterial)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf(`{"importJob":%q,"algorithm":"AES_256_KWP","wrappedKey":%q}`,
					kmsName+"/importJobs/job-hsm", b64(wrapped)), nil
			},
			Save: map[string]string{"kwpVersion": "name"}},
		// Trusted wrapping is only valid for HSM_SINGLE_TENANT keys and, per
		// Discovery, never for ENCRYPT_DECRYPT; the target imports a raw AES key.
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=trusted&skipInitialVersionCreation=true",
			Body: `{"purpose":"RAW_ENCRYPT_DECRYPT","importOnly":true,"versionTemplate":{"algorithm":"AES_256_GCM","protectionLevel":"HSM_SINGLE_TENANT"}}`},
		Scenario{Service: "kms", Method: "POST",
			Path: kmsBase + "/cryptoKeys/trusted/cryptoKeyVersions:importTrustedKeyWrappedCryptoKeyVersion",
			Body: fmt.Sprintf(`{"importingKey":"${kwpVersion}","algorithm":"AES_256_GCM","wrappedKey":%q}`,
				b64(trustedWrapped)),
			Save: map[string]string{"kmsTrustedVersion": "name"}},
		Scenario{Service: "kms", Method: "GET",
			Path: "/v1/${kmsTrustedVersion}:exportTrustedKeyWrappedCryptoKeyVersion?wrappingKey=${kwpVersion}"},

		// Deletion: a version must be created, destroyed, past its destruction
		// window, deleted, and only then can the crypto key be deleted. The
		// 30-day window is crossed on the emulator clock (744h = 31 days).
		Scenario{Service: "kms", Method: "POST", Path: kmsBase + "/cryptoKeys?cryptoKeyId=del",
			Body: `{"purpose":"ENCRYPT_DECRYPT"}`},
		Scenario{Service: "kms", Method: "POST",
			Path: kmsBase + "/cryptoKeys/del/cryptoKeyVersions/1:destroy", Body: `{}`},
		Scenario{Service: "kms", Method: "GET",
			Path: kmsBase + "/cryptoKeys/del/cryptoKeyVersions/1", AdvanceClock: "744h"},
		Scenario{Service: "kms", Method: "DELETE",
			Path: kmsBase + "/cryptoKeys/del/cryptoKeyVersions/1"},
		Scenario{Service: "kms", Method: "DELETE", Path: kmsBase + "/cryptoKeys/del", AdvanceClock: "real"},
	)

	// ─── IAM ──────────────────────────────────────────────────────────────────

	saBase := "/v1/projects/" + p + "/serviceAccounts"
	sc = append(sc,
		Scenario{Service: "iam", Method: "POST", Path: saBase + "?accountId=" + sa,
			Body: `{"serviceAccount":{"displayName":"Conformance SA"}}`},
		Scenario{Service: "iam", Method: "GET", Path: saBase + "/" + saEmail},
		Scenario{Service: "iam", Method: "GET", Path: saBase},
		Scenario{Service: "iam", Method: "POST", Path: saBase + "/" + saEmail + "/keys", Body: `{}`,
			Save: map[string]string{"keyName": "name"}},
		Scenario{Service: "iam", Method: "GET", Path: saBase + "/" + saEmail + "/keys"},
		// keys.get / keys.disable / keys.enable: disable/enable declare an Empty
		// response, so the state they set is validated through the get that
		// follows each verb (and the Empty body itself against Discovery).
		Scenario{Service: "iam", Method: "GET", Path: "/v1/${keyName}"},
		Scenario{Service: "iam", Method: "POST", Path: "/v1/${keyName}:disable"},
		Scenario{Service: "iam", Method: "GET", Path: "/v1/${keyName}"},
		Scenario{Service: "iam", Method: "POST", Path: "/v1/${keyName}:enable"},
		Scenario{Service: "iam", Method: "GET", Path: "/v1/${keyName}"},
		Scenario{Service: "iam", Method: "POST", Path: saBase + "/" + saEmail + ":signBlob",
			Body: `{"bytesToSign":"aGVsbG8="}`},
		Scenario{Service: "iam", Method: "GET", Path: saBase + "/missing-" + suffix + "@" + p + ".iam.gserviceaccount.com"},
		Scenario{Service: "iam", Method: "DELETE", Path: "/v1/${keyName}"},
		Scenario{Service: "iam", Method: "DELETE", Path: saBase + "/" + saEmail},
	)

	// ─── IAM Service Account Credentials ──────────────────────────────────────
	// The unique iamcredentials verbs. signBlob/signJwt share their path with
	// iam and are exercised by the IAM block above.
	sc = append(sc,
		Scenario{Service: "iamcredentials", Method: "POST",
			Path: "/v1/projects/-/serviceAccounts/" + saEmail + ":generateAccessToken",
			Body: `{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"3600s"}`},
		Scenario{Service: "iamcredentials", Method: "POST",
			Path: "/v1/projects/-/serviceAccounts/" + saEmail + ":generateIdToken",
			Body: `{"audience":"https://example.com","includeEmail":true}`},
	)

	// ─── Cloud DNS ────────────────────────────────────────────────────────────
	dnsBase := "/dns/v1/projects/" + p + "/managedZones"
	sc = append(sc,
		Scenario{Service: "clouddns", Method: "POST", Path: dnsBase,
			Body: fmt.Sprintf(`{"name":%q,"dnsName":%q,"description":"conformance"}`, zone, dnsName)},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase + "/" + zone},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase},
		Scenario{Service: "clouddns", Method: "GET", Path: "/dns/v1/projects/" + p},
		Scenario{Service: "clouddns", Method: "POST", Path: dnsBase + "/" + zone + "/rrsets",
			Body: fmt.Sprintf(`{"name":%q,"type":"A","ttl":300,"rrdatas":["1.2.3.4"]}`, "www."+dnsName)},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase + "/" + zone + "/rrsets"},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase + "/missing-" + suffix},
		Scenario{Service: "clouddns", Method: "DELETE", Path: dnsBase + "/" + zone},
	)

	// ─── BigQuery ─────────────────────────────────────────────────────────────
	// Executed-SQL coverage: the emulator evaluates a bounded Standard SQL
	// subset (BQ1/BQ2), so the wire harness exercises SELECT, DDL (CREATE TABLE
	// AS SELECT), DML (INSERT) and the fail-loud error path alongside the
	// metadata + insertAll surface. The job ids captured from the jobs.query
	// responses feed getQueryResults and jobs.get.
	bqBase := "/bigquery/v2/projects/" + p
	bqTbl2 := "conf_tbl2_" + suffix
	bqLoadTbl := "conf_loadtbl_" + suffix
	bqLoadBucket := "conf-load-" + suffix
	// Uploaded-file load (BQL2): the multipart job-resource part names the
	// destination, and the media part carries the CSV bytes.
	bqUploadTbl := "conf_uploadtbl_" + suffix
	bqUploadBoundary := "jaiscloud-bq-" + suffix
	bqUploadJob := "conf-upload-" + suffix
	// Parquet/Avro gs:// loads (BQL3): embedded-schema formats.
	bqParquetTbl := "conf_parquettbl_" + suffix
	bqParquetBucket := "conf-parquet-" + suffix
	bqAvroTbl := "conf_avrotbl_" + suffix
	bqAvroBucket := "conf-avro-" + suffix
	bqUploadJSON := fmt.Sprintf(
		`{"jobReference":{"projectId":%q,"jobId":%q},"configuration":{"load":{"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":%q},"sourceFormat":"CSV","writeDisposition":"WRITE_APPEND"}}}`,
		p, bqUploadJob, p, ds, bqUploadTbl)
	bqUploadBody := "--" + bqUploadBoundary + "\r\n" +
		"Content-Type: application/json; charset=UTF-8\r\n\r\n" +
		bqUploadJSON + "\r\n" +
		"--" + bqUploadBoundary + "\r\n" +
		"Content-Type: */*\r\n\r\n" +
		"1,alice\n" +
		"\r\n--" + bqUploadBoundary + "--\r\n"
	sc = append(sc,
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, p, ds)},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"}]}}`, p, ds, tbl)},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl + "/insertAll",
			Body: `{"rows":[{"insertId":"1","json":{"id":"1"}}]}`},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl + "/data"},
		// Load job (BQL1): stage a CSV row in the emulated GCS, then jobs.insert a
		// configuration.load that reads it back through the same gs:// path the
		// official clients use. (CSV rather than NDJSON: the wire harness parses
		// any JSON-looking upload body as an objects.insert request, a
		// media-upload false positive; plain CSV text is skipped.)
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q}`, bqLoadBucket)},
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + bqLoadBucket + "/o?uploadType=media&name=load.csv",
			Body:        "5,alice\n",
			ContentType: "text/plain"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"name","type":"STRING"}]}}`, p, ds, bqLoadTbl)},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/jobs",
			Body: fmt.Sprintf(`{"jobReference":{"projectId":%q,"jobId":%q},"configuration":{"load":{"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":%q},"sourceUris":[%q],"sourceFormat":"CSV","writeDisposition":"WRITE_APPEND"}}}`,
				p, "conf-load-"+suffix, p, ds, bqLoadTbl, "gs://"+bqLoadBucket+"/load.csv"),
			Save: map[string]string{"bqLoadJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/jobs/${bqLoadJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + bqLoadTbl + "/data"},

		// Uploaded-file load (BQL2): a multipart/related jobs.insert media
		// upload (load_table_from_file with a known size). The job resource is
		// the JSON part; the media part carries the CSV bytes.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"name","type":"STRING"}]}}`, p, ds, bqUploadTbl)},
		Scenario{Service: "bigquery", Method: "POST",
			Path:        "/upload/bigquery/v2/projects/" + p + "/jobs?uploadType=multipart",
			Body:        bqUploadBody,
			ContentType: "multipart/related; boundary=" + bqUploadBoundary,
			Save:        map[string]string{"bqUploadJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/jobs/${bqUploadJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + bqUploadTbl + "/data"},

		// Parquet gs:// load (BQL3): the destination schema is derived from the
		// file's embedded Parquet schema (no configuration.load.schema).
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q}`, bqParquetBucket)},
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + bqParquetBucket + "/o?uploadType=media&name=rows.parquet",
			Body:        string(loadParquetFixture()),
			ContentType: "application/octet-stream"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/jobs",
			Body: fmt.Sprintf(`{"jobReference":{"projectId":%q,"jobId":%q},"configuration":{"load":{"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":%q},"sourceUris":[%q],"sourceFormat":"PARQUET"}}}`,
				p, "conf-parquet-"+suffix, p, ds, bqParquetTbl, "gs://"+bqParquetBucket+"/rows.parquet"),
			Save: map[string]string{"bqParquetJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/jobs/${bqParquetJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + bqParquetTbl + "/data"},

		// Avro gs:// load (BQL3): same embedded-schema path as Parquet.
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q}`, bqAvroBucket)},
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + bqAvroBucket + "/o?uploadType=media&name=rows.avro",
			Body:        string(loadAvroFixture()),
			ContentType: "application/octet-stream"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/jobs",
			Body: fmt.Sprintf(`{"jobReference":{"projectId":%q,"jobId":%q},"configuration":{"load":{"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":%q},"sourceUris":[%q],"sourceFormat":"AVRO"}}}`,
				p, "conf-avro-"+suffix, p, ds, bqAvroTbl, "gs://"+bqAvroBucket+"/rows.avro"),
			Save: map[string]string{"bqAvroJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/jobs/${bqAvroJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + bqAvroTbl + "/data"},

		// SELECT over the stored row; capture the job id for the read-back calls.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"SELECT id FROM `%s.%s.%s` WHERE id = 1\",\"useLegacySql\":false}", p, ds, tbl),
			Save: map[string]string{"bqSelectJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/queries/${bqSelectJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/jobs/${bqSelectJob}"},
		// DDL: CREATE TABLE AS SELECT.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"CREATE TABLE `%s.%s.%s` AS SELECT id FROM `%s.%s.%s`\",\"useLegacySql\":false}", p, ds, bqTbl2, p, ds, tbl),
			Save: map[string]string{"bqDdlJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/queries/${bqDdlJob}"},
		// DML: INSERT INTO the CTAS table, then read it back.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"INSERT INTO `%s.%s.%s` (id) VALUES (2)\",\"useLegacySql\":false}", p, ds, bqTbl2),
			Save: map[string]string{"bqDmlJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/queries/${bqDmlJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + bqTbl2 + "/data"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries", Body: `{"query":"SELECT 1"}`},
		// Fail loud: a construct outside the frozen subset is a 400 invalidQuery.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"MERGE `%s.%s.%s` T USING `%s.%s.%s` S ON T.id = S.id\",\"useLegacySql\":false}", p, ds, tbl, p, ds, tbl)},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/missing_" + suffix},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + bqTbl2},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + bqLoadTbl},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + bqUploadTbl},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + bqParquetTbl},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + bqAvroTbl},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds},
	)

	// ─── Cloud Datastore (REST data plane) ────────────────────────────────────
	dsA := "conf-ds-a-" + suffix
	dsTxn := "conf-ds-txn-" + suffix
	dsKey := func(name string) string {
		return fmt.Sprintf(`{"partitionId":{"projectId":%q},"path":[{"kind":"ConfDsTask","name":%q}]}`, p, name)
	}
	sc = append(sc,
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":commit",
			// The null property (top level and inside an array) exercises the
			// google.protobuf.NullValue wire form: real GCP renders it as JSON
			// null (protojson), which the validator accepts via the NullValue
			// enum carve-out in validate.go.
			Body: fmt.Sprintf(`{"mode":"NON_TRANSACTIONAL","mutations":[{"upsert":{"key":%s,"properties":{"n":{"integerValue":"1"},"nul":{"nullValue":null},"arr":{"arrayValue":{"values":[{"nullValue":null}]}}}}}]}`, dsKey(dsA))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":lookup",
			Body: fmt.Sprintf(`{"keys":[%s]}`, dsKey(dsA))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":lookup",
			Body: fmt.Sprintf(`{"keys":[%s]}`, dsKey("conf-ds-missing-"+suffix))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":runQuery",
			Body: `{"query":{"kind":[{"name":"ConfDsTask"}],"filter":{"propertyFilter":{"property":{"name":"n"},"op":"EQUAL","value":{"integerValue":"1"}}}}}`},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":runAggregationQuery",
			Body: `{"aggregationQuery":{"nestedQuery":{"kind":[{"name":"ConfDsTask"}]},"aggregations":[{"alias":"total","count":{}}]}}`},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":allocateIds",
			Body: fmt.Sprintf(`{"keys":[{"partitionId":{"projectId":%q},"path":[{"kind":"ConfDsTask"}]}]}`, p)},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":reserveIds",
			Body: fmt.Sprintf(`{"keys":[{"partitionId":{"projectId":%q},"path":[{"kind":"ConfDsTask","id":"999999999"}]}]}`, p)},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":beginTransaction", Body: `{}`,
			Save: map[string]string{"txn": "transaction"}},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":commit",
			Body: fmt.Sprintf(`{"mode":"TRANSACTIONAL","transaction":"${txn}","mutations":[{"insert":{"key":%s,"properties":{"n":{"integerValue":"2"}}}}]}`, dsKey(dsTxn))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":rollback", Body: `{}`},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":runQuery",
			Body: `{"gqlQuery":{"queryString":"SELECT * FROM ConfDsTask"}}`},
	)

	// ─── Cloud Logging (REST data plane) ──────────────────────────────────────
	logID := "conf-log-" + suffix
	logName := "projects/" + p + "/logs/" + logID
	logFilter := fmt.Sprintf(`logName=\"%s\"`, logName)
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/entries:write",
			Body: fmt.Sprintf(`{"logName":%q,"resource":{"type":"global"},"labels":{"suite":"conformance"},"entries":[{"severity":"INFO","textPayload":"hello"},{"severity":"ERROR","jsonPayload":{"msg":"boom"}}]}`, logName)},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/entries:list",
			Body: fmt.Sprintf(`{"resourceNames":["projects/%s"],"filter":"%s"}`, p, logFilter)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/logs"},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/monitoredResourceDescriptors?pageSize=5"},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + logName},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/entries:list",
			Body: fmt.Sprintf(`{"resourceNames":["projects/%s"],"filter":"%s"}`, p, logFilter)},
	)

	// Cloud Logging config plane (sinks + exclusions).
	sinkID := "conf-sink-" + suffix
	sinkName := "projects/" + p + "/sinks/" + sinkID
	sinkDest := "storage.googleapis.com/conf-bucket-" + suffix
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/projects/" + p + "/sinks",
			Body: fmt.Sprintf(`{"name":%q,"destination":%q,"filter":"severity>=WARNING"}`, sinkID, sinkDest)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + sinkName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/sinks"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + sinkName + "?updateMask=filter",
			Body: `{"filter":"severity>=ERROR"}`},
		Scenario{Service: "logging", Method: "PUT", Path: "/v2/" + sinkName,
			Body: fmt.Sprintf(`{"name":%q,"destination":%q,"filter":"severity>=ERROR","description":"updated"}`, sinkID, sinkDest)},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + sinkName},
	)
	exclID := "conf-exclusion-" + suffix
	exclName := "projects/" + p + "/exclusions/" + exclID
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/projects/" + p + "/exclusions",
			Body: fmt.Sprintf(`{"name":%q,"filter":"severity<DEBUG"}`, exclID)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + exclName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/exclusions"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + exclName + "?updateMask=disabled",
			Body: `{"disabled":true}`},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + exclName},
	)

	// Cloud Logging logs-based metrics.
	metricID := "conf-metric-" + suffix
	metricName := "projects/" + p + "/metrics/" + metricID
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/projects/" + p + "/metrics",
			Body: fmt.Sprintf(`{"name":%q,"description":"conformance","filter":"severity>=ERROR","valueExtractor":"EXTRACT(jsonPayload.latency)","labelExtractors":{"code":"EXTRACT(jsonPayload.code)"},"metricDescriptor":{"valueType":"DISTRIBUTION","unit":"ms","labels":[{"key":"code","valueType":"INT64"}]},"bucketOptions":{"linearBuckets":{"numFiniteBuckets":3,"width":1,"offset":0}}}`, metricID)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + metricName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/metrics"},
		Scenario{Service: "logging", Method: "PUT", Path: "/v2/" + metricName,
			Body: fmt.Sprintf(`{"name":%q,"filter":"severity>=WARNING","valueExtractor":"EXTRACT(jsonPayload.latency)","labelExtractors":{"code":"EXTRACT(jsonPayload.code)"},"metricDescriptor":{"valueType":"DISTRIBUTION","unit":"ms","labels":[{"key":"code","valueType":"INT64"}]}}`, metricID)},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + metricName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + metricName},
	)

	// Cloud Logging Admin v2: buckets, views (+ IAM), links, log scopes, and the
	// per-scope settings/CMEK records. One project bucket holds the views and
	// links; a second exercises the async create/update and delete/undelete.
	logLocParent := "projects/" + p + "/locations/global"
	logBucketID := "conf-bucket-" + suffix
	logBucket := logLocParent + "/buckets/" + logBucketID
	logBucket2ID := "conf-bucket2-" + suffix
	logBucket2 := logLocParent + "/buckets/" + logBucket2ID
	logViewID := "conf-view-" + suffix
	logView := logBucket + "/views/" + logViewID
	logLinkID := "conf-link-" + suffix
	logLink := logBucket + "/links/" + logLinkID
	logScopeID := "conf-scope-" + suffix
	logScope := logLocParent + "/logScopes/" + logScopeID
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logLocParent + "/buckets?bucketId=" + logBucketID,
			Body: `{"description":"conformance","retentionDays":30}`},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logLocParent + "/buckets"},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logBucket},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + logBucket + "?updateMask=description",
			Body: `{"description":"updated"}`},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logLocParent + "/buckets:createAsync?bucketId=" + logBucket2ID,
			Body: `{"description":"async"}`},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logBucket2 + ":updateAsync?updateMask=description",
			Body: `{"description":"async-updated"}`},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + logBucket2},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logBucket2},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logBucket2 + ":undelete"},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logBucket + "/views?viewId=" + logViewID,
			Body: `{"description":"conformance","filter":"severity>=ERROR"}`},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logView},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logBucket + "/views"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + logView + "?updateMask=description",
			Body: `{"description":"updated"}`},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logView + ":getIamPolicy", Body: `{}`},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logView + ":setIamPolicy",
			Body: `{"policy":{"bindings":[{"role":"roles/viewer","members":["user:test@example.com"]}]}}`},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logView + ":testIamPermissions",
			Body: `{"permissions":["logging.views.get"]}`},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + logView},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logBucket + "/links?linkId=" + logLinkID,
			Body: fmt.Sprintf(`{"description":"conformance","bigqueryDataset":{"datasetId":"conf_ds_%s"}}`, suffix)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logLink},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logBucket + "/links"},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + logLink},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/" + logLocParent + "/logScopes?logScopeId=" + logScopeID,
			Body: fmt.Sprintf(`{"description":"conformance","resourceNames":["projects/%s"]}`, p)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logScope},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + logLocParent + "/logScopes"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + logScope + "?updateMask=description",
			Body: `{"description":"updated"}`},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + logScope},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/settings"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/projects/" + p + "/settings?updateMask=storageLocation",
			Body: `{"storageLocation":"us"}`},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/cmekSettings"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/projects/" + p + "/cmekSettings?updateMask=kmsKeyName",
			Body: `{"kmsKeyName":"projects/p/locations/global/keyRings/r/cryptoKeys/k"}`},
	)

	// ─── Cloud Functions v2 runtime catalog ───────────────────────────────────
	// The runtime catalog is the v2-deploy enabler: gcloud resolves a function's
	// runtime from this list (filtered to environment GEN_2) before uploading
	// source. Both the unfiltered list and an AIP-160 name filter are exercised.
	sc = append(sc,
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/projects/" + p + "/locations/us-central1/runtimes"},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/projects/" + p + "/locations/us-central1/runtimes?filter=" + url.QueryEscape(`name="nodejs20"`)},
	)

	// ─── Cloud Functions common APIs + long-running operations ────────────────
	// GetLocation is the shared google.cloud.location.Locations method and
	// CancelOperation/DeleteOperation/WaitOperation are the shared
	// google.longrunning.Operations methods. None are enumerated in the service
	// Discovery document (the classifier upgrades those cells in
	// docs/fidelity-overrides.yaml), so these scenarios pin the served shapes.
	// A create first captures the persisted operation name so get/list/wait/
	// cancel/delete exercise a real stored operation (unknown ids are NotFound).
	fnID := "conf-fn-" + suffix
	sc = append(sc,
		Scenario{Service: "functions", Method: "GET",
			Path: "/v1/projects/" + p + "/locations/us-central1"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/projects/" + p + "/locations/us-central1/functions?functionId=" + url.QueryEscape(fnID),
			Body: `{"buildConfig":{"runtime":"nodejs20","entryPoint":"handler"}}`,
			Save: map[string]string{"fnop": "name"}},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/${fnop}"},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/projects/" + p + "/locations/us-central1/operations"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${fnop}:wait"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${fnop}:cancel"},
		Scenario{Service: "functions", Method: "DELETE",
			Path: "/v2/${fnop}"},
	)

	// ─── Cloud Functions v2 upgrade / traffic control plane (FD5) ─────────────
	// The seven 1st→2nd gen upgrade verbs are custom POST methods on a function
	// name (REST-only: documented as FunctionService RPCs, but absent from the
	// public googleapis proto mirror and all generated clients). A fresh function
	// is upgraded through
	// setup → redirect → rollback → redirect → abort, then setup → redirect →
	// commit, then setup → redirect → commitAsGen2 → detach, then deleted.
	upgID := "conf-upg-" + suffix
	sc = append(sc,
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/projects/" + p + "/locations/us-central1/functions?functionId=" + url.QueryEscape(upgID),
			Body: `{"buildConfig":{"runtime":"nodejs20","entryPoint":"handler"}}`,
			Save: map[string]string{"upgFn": "response.name"}},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${upgFn}:setupFunctionUpgradeConfig",
			Body: `{"buildConfigOverrides":{"runtime":"nodejs22"},"serviceConfigOverrides":{"maxInstanceCount":4}}`},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:rollbackFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:abortFunctionUpgrade"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${upgFn}:setupFunctionUpgradeConfig",
			Body: `{"buildConfigOverrides":{"runtime":"nodejs22"}}`},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:commitFunctionUpgrade"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:setupFunctionUpgradeConfig"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:commitFunctionUpgradeAsGen2"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:detachFunction"},
		Scenario{Service: "functions", Method: "DELETE", Path: "/v2/${upgFn}"},
	)

	// ─── Cloud Monitoring (REST data plane) ───────────────────────────────────
	mType := "conf.metric_" + suffix
	tsType := "conf.ts_" + suffix
	tsFilter := url.QueryEscape(`metric.type="` + tsType + `"`)
	monBase := "/v3/projects/" + p
	sc = append(sc,
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/metricDescriptors",
			Body: fmt.Sprintf(`{"type":%q,"metricKind":"GAUGE","valueType":"INT64","description":"conformance","displayName":"Conf Metric","labels":[{"key":"env","valueType":"STRING","description":"environment"}]}`, mType)},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/metricDescriptors"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/metricDescriptors/" + mType},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/timeSeries",
			Body: fmt.Sprintf(`{"timeSeries":[{"metric":{"type":%q,"labels":{"k":"v"}},"resource":{"type":"global"},"metricKind":"GAUGE","valueType":"DOUBLE","points":[{"interval":{"endTime":"2026-01-01T00:00:00Z"},"value":{"doubleValue":1.5}}]}]}`, tsType)},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/timeSeries?filter=" + tsFilter},
		// The folders arm of timeSeries.list: the emulator has no folder→project
		// membership, so it aggregates the same series store across projects.
		Scenario{Service: "monitoring", Method: "GET", Path: "/v3/folders/123456789012/timeSeries?filter=" + tsFilter},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/timeSeries:createService",
			Body: fmt.Sprintf(`{"timeSeries":[{"metric":{"type":%q},"resource":{"type":"global"},"metricKind":"GAUGE","valueType":"DOUBLE","points":[{"interval":{"endTime":"2026-01-01T00:00:00Z"},"value":{"doubleValue":2.5}}]}]}`, tsType)},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/monitoredResourceDescriptors"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/monitoredResourceDescriptors/gce_instance"},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/alertPolicies",
			Body: fmt.Sprintf(`{"displayName":"Conf Policy %s","combiner":"OR","conditions":[{"displayName":"cond","conditionThreshold":{"filter":%q,"comparison":"COMPARISON_GT","thresholdValue":1,"duration":"60s","trigger":{"count":1}}}]}`, suffix, `metric.type="`+tsType+`"`),
			Save: map[string]string{"policyName": "name"}},
		Scenario{Service: "monitoring", Method: "GET", Path: "/v3/${policyName}"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/alertPolicies"},
		Scenario{Service: "monitoring", Method: "PATCH", Path: "/v3/${policyName}?updateMask=displayName",
			Body: `{"displayName":"Updated Conf Policy"}`},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/notificationChannels",
			Body: fmt.Sprintf(`{"type":"email","displayName":"Conf Channel %s","labels":{"email_address":"conf@example.com"}}`, suffix),
			Save: map[string]string{"channelName": "name"}},
		Scenario{Service: "monitoring", Method: "GET", Path: "/v3/${channelName}"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/notificationChannels"},
		Scenario{Service: "monitoring", Method: "POST", Path: "/v3/${channelName}:sendVerificationCode"},
		Scenario{Service: "monitoring", Method: "POST", Path: "/v3/${channelName}:getVerificationCode"},
		Scenario{Service: "monitoring", Method: "POST", Path: "/v3/${channelName}:verify"},
		Scenario{Service: "monitoring", Method: "PATCH", Path: "/v3/${channelName}?updateMask=description",
			Body: `{"description":"updated description"}`},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/notificationChannelDescriptors"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/notificationChannelDescriptors/email"},
		Scenario{Service: "monitoring", Method: "DELETE", Path: "/v3/${channelName}"},
		Scenario{Service: "monitoring", Method: "DELETE", Path: "/v3/${policyName}"},
		Scenario{Service: "monitoring", Method: "DELETE", Path: monBase + "/metricDescriptors/" + mType},
	)

	// ─── Google Kubernetes Engine (GKE) v1 ────────────────────────────────────
	// GKE shares the canonical /v1/projects/{p}/locations/{l}/clusters path with
	// Managed Kafka on one origin, so the recorder reaches it through the
	// /container path prefix (Terraform/gcloud path-mode routing; the SDK
	// host-mode form is covered by the adapter/router unit test). A cluster is
	// created (returns a CREATE_CLUSTER Operation), read, listed, its operation
	// is fetched and listed, then it is deleted (DELETE_CLUSTER Operation) and a
	// post-delete read proves the NotFound error envelope.
	gkeCluster := "conf-gke-" + suffix
	gkePool := "conf-gke-pool-" + suffix
	gkeBase := "/container/v1/projects/" + p + "/locations/us-central1"
	gkeClusterName := gkeBase + "/clusters/" + gkeCluster
	gkePoolsBase := gkeClusterName + "/nodePools"
	gkePoolName := gkePoolsBase + "/" + gkePool
	gkeInline := "conf-gke-inline-" + suffix
	gkeInlinePool := "conf-gke-inline-pool-" + suffix
	gkeInlineName := gkeBase + "/clusters/" + gkeInline
	gkeLegacy := "conf-gke-nodeconfig-" + suffix
	gkeLegacyName := gkeBase + "/clusters/" + gkeLegacy
	gkeConflictInc := "conf-gke-conflict-inc-" + suffix
	gkeConflictNC := "conf-gke-conflict-nc-" + suffix
	sc = append(sc,
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":1}}`, gkeCluster),
			Save: map[string]string{"gkeOp": "name"}},
		Scenario{Service: "container", Method: "GET", Path: gkeClusterName},
		Scenario{Service: "container", Method: "GET", Path: gkeBase + "/clusters"},
		Scenario{Service: "container", Method: "GET", Path: gkeBase + "/operations/${gkeOp}"},
		Scenario{Service: "container", Method: "GET", Path: gkeBase + "/operations"},
		// Node-pool CRUD: create, list, get, a version update, the size/
		// autoscaling/management setters, a rollback, then delete and a
		// post-delete read proving the NotFound envelope.
		Scenario{Service: "container", Method: "POST", Path: gkePoolsBase,
			Body: fmt.Sprintf(`{"nodePool":{"name":%q,"initialNodeCount":1}}`, gkePool)},
		Scenario{Service: "container", Method: "GET", Path: gkePoolsBase},
		Scenario{Service: "container", Method: "GET", Path: gkePoolName},
		Scenario{Service: "container", Method: "PUT", Path: gkePoolName,
			Body: `{"nodeVersion":"1.31.0-gke.100"}`},
		Scenario{Service: "container", Method: "POST", Path: gkePoolName + ":setSize",
			Body: `{"nodeCount":2}`},
		Scenario{Service: "container", Method: "POST", Path: gkePoolName + ":setAutoscaling",
			Body: `{"autoscaling":{"enabled":true,"minNodeCount":1,"maxNodeCount":3}}`},
		Scenario{Service: "container", Method: "POST", Path: gkePoolName + ":setManagement",
			Body: `{"management":{"autoUpgrade":true,"autoRepair":true}}`},
		Scenario{Service: "container", Method: "POST", Path: gkePoolName + ":rollback"},
		Scenario{Service: "container", Method: "DELETE", Path: gkePoolName},
		Scenario{Service: "container", Method: "GET", Path: gkePoolName},
		// Cluster setters: labels, addons, a ClusterUpdate, master version and
		// the IP-rotation pair.
		Scenario{Service: "container", Method: "POST", Path: gkeClusterName + ":setResourceLabels",
			Body: `{"resourceLabels":{"team":"platform"}}`},
		Scenario{Service: "container", Method: "POST", Path: gkeClusterName + ":setAddons",
			Body: `{"addonsConfig":{"httpLoadBalancing":{"disabled":true}}}`},
		Scenario{Service: "container", Method: "POST", Path: gkeClusterName + ":setLogging",
			Body: `{"loggingService":"logging.googleapis.com/kubernetes"}`},
		Scenario{Service: "container", Method: "PUT", Path: gkeClusterName,
			Body: `{"update":{"desiredMonitoringService":"monitoring.googleapis.com/kubernetes","desiredShieldedNodes":{"enabled":false}}}`},
		// A read straight after the update proves the ClusterUpdate fields applied.
		Scenario{Service: "container", Method: "GET", Path: gkeClusterName},
		Scenario{Service: "container", Method: "POST", Path: gkeClusterName + ":updateMaster",
			Body: `{"masterVersion":"1.31.0-gke.100"}`},
		Scenario{Service: "container", Method: "POST", Path: gkeClusterName + ":startIpRotation"},
		Scenario{Service: "container", Method: "POST", Path: gkeClusterName + ":completeIpRotation"},
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/operations/${gkeOp}:cancel"},
		Scenario{Service: "container", Method: "DELETE", Path: gkeClusterName},
		Scenario{Service: "container", Method: "GET", Path: gkeClusterName},
		// DPG49: a negative cluster-level initialNodeCount is InvalidArgument
		// only when the request carries no nodePools (captured against real GKE
		// in docs/gcp/gke-create-pool-conflict.md).
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":-1}}`, gkeCluster+"-neg")},
		// Inline nodePools replace the generated default-pool: the create is
		// accepted and the pools materialise, so the cluster read/list and each
		// pool read back the caller's shape.
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"nodePools":[{"name":%q,"initialNodeCount":1,"config":{"machineType":"e2-micro"}}]}}`,
				gkeInline, gkeInlinePool)},
		Scenario{Service: "container", Method: "GET", Path: gkeInlineName},
		Scenario{Service: "container", Method: "GET", Path: gkeInlineName + "/nodePools"},
		Scenario{Service: "container", Method: "GET", Path: gkeInlineName + "/nodePools/" + gkeInlinePool},
		Scenario{Service: "container", Method: "DELETE", Path: gkeInlineName},
		// The legacy default-pool shape: initialNodeCount + nodeConfig with no
		// nodePools generates default-pool carrying the caller's config (DPG42),
		// and the cluster read echoes nodeConfig from that first pool.
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":2,"nodeConfig":{"machineType":"e2-micro"}}}`,
				gkeLegacy)},
		Scenario{Service: "container", Method: "GET", Path: gkeLegacyName},
		Scenario{Service: "container", Method: "GET", Path: gkeLegacyName + "/nodePools"},
		Scenario{Service: "container", Method: "GET", Path: gkeLegacyName + "/nodePools/default-pool"},
		Scenario{Service: "container", Method: "DELETE", Path: gkeLegacyName},
		// The two default-pool conflicts real GKE 400s, verbatim from
		// docs/gcp/gke-create-pool-conflict.md: initialNodeCount>0 first, then
		// nodeConfig (node_config wins when both are set; the gRPC check covers
		// that precedence). Nothing is created by either.
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":1,"nodePools":[{"name":"pool-a","initialNodeCount":1}]}}`,
				gkeConflictInc)},
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"nodeConfig":{"machineType":"e2-micro"},"nodePools":[{"name":"pool-a","initialNodeCount":1}]}}`,
				gkeConflictNC)},
	)

	// ─── Cloud Run Admin v2 (run.googleapis.com) ──────────────────────────────
	// Cloud Run shares the /v2/projects/{p}/locations/{l} namespace with Cloud
	// Functions/Tasks, so the recorder uses canonical paths that detect as run:
	// the services/revisions family and the run-prefixed operation id. A service
	// is created (returns a done google.longrunning.Operation whose response is
	// the Service), read, listed, its revision listed/fetched, its IAM policy
	// set/read/tested, a description-only PATCH applied, its operation fetched,
	// then it is deleted and a post-delete read proves the NotFound envelope.
	// The bare .../operations list is intentionally NOT recorded: that path is
	// path-ambiguous with Cloud Functions on one origin and stays with functions
	// by design (documented limitation; run operations are reachable by their
	// prefixed id).
	runSvc := "conf-run-" + suffix
	runBase := "/v2/projects/" + p + "/locations/us-central1"
	sc = append(sc,
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services?serviceId=" + runSvc,
			Body: `{"template":{"containers":[{"image":"nginx:latest","ports":[{"containerPort":80}]}]}}`,
			Save: map[string]string{"runOp": "name", "runRev": "response.latestReadyRevision"}},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services"},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc + "/revisions"},
		Scenario{Service: "run", Method: "GET", Path: "/v2/${runRev}"},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc + ":getIamPolicy"},
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services/" + runSvc + ":setIamPolicy",
			Body: `{"policy":{"bindings":[{"role":"roles/run.invoker","members":["allUsers"]}]}}`},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc + ":getIamPolicy"},
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services/" + runSvc + ":testIamPermissions",
			Body: `{"permissions":["run.services.get","run.services.delete"]}`},
		Scenario{Service: "run", Method: "PATCH", Path: runBase + "/services/" + runSvc + "?updateMask=description",
			Body: `{"description":"updated"}`},
		// A template-changing update mints revision {svc}-00002, retiring the
		// first revision captured as ${runRev}; deleting a retired revision is
		// the only form the API permits (revisions.delete).
		Scenario{Service: "run", Method: "PATCH", Path: runBase + "/services/" + runSvc + "?updateMask=template",
			Body: `{"template":{"containers":[{"image":"nginx:latest","ports":[{"containerPort":80}]}]}}`},
		Scenario{Service: "run", Method: "DELETE", Path: "/v2/${runRev}"},
		// The shared google.longrunning.Operations :cancel verb is served on the
		// run surface (the run v2 Discovery document has no operations.cancel, so
		// docs/fidelity-overrides.yaml upgrades the cell to ga). It validates a
		// stored operation, so it runs before the operation is read back.
		Scenario{Service: "run", Method: "POST", Path: "/v2/${runOp}:cancel"},
		Scenario{Service: "run", Method: "GET", Path: "/v2/${runOp}"},
		// operations.delete addresses the operation record itself, so it runs
		// before the service it belongs to is deleted.
		Scenario{Service: "run", Method: "DELETE", Path: "/v2/${runOp}"},
		Scenario{Service: "run", Method: "DELETE", Path: runBase + "/services/" + runSvc},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc},
	)

	// Dry-run / optimistic-concurrency probes, each self-contained so they do
	// not perturb the lifecycle above: a validateOnly create previews but does
	// not persist; a stale etag on delete is ABORTED and leaves the service; an
	// allowMissing update upserts; a validateOnly delete keeps the service.
	drySvc := "conf-run-dry-" + suffix
	occSvc := "conf-run-occ-" + suffix
	missSvc := "conf-run-um-" + suffix
	sc = append(sc,
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services?serviceId=" + drySvc + "&validateOnly=true",
			Body: `{"template":{"containers":[{"image":"nginx:latest","ports":[{"containerPort":80}]}]}}`},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + drySvc},
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services?serviceId=" + occSvc,
			Body: `{"template":{"containers":[{"image":"nginx:latest"}]}}`,
			Save: map[string]string{"runEtag": "response.etag"}},
		Scenario{Service: "run", Method: "DELETE", Path: runBase + "/services/" + occSvc + "?etag=stale-token"},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + occSvc},
		Scenario{Service: "run", Method: "DELETE", Path: runBase + "/services/" + occSvc + "?etag=${runEtag}"},
		Scenario{Service: "run", Method: "PATCH", Path: runBase + "/services/" + missSvc + "?updateMask=template&allowMissing=true",
			Body: `{"template":{"containers":[{"image":"nginx:latest"}]}}`},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + missSvc},
		Scenario{Service: "run", Method: "DELETE", Path: runBase + "/services/" + missSvc + "?validateOnly=true"},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + missSvc},
		Scenario{Service: "run", Method: "DELETE", Path: runBase + "/services/" + missSvc},
	)

	// ─── Service Usage ────────────────────────────────────────────────────────
	// Enable one service, then BatchGet it alongside a never-enabled one. batchGet
	// carries its `names` as repeated query parameters (full resource names), so
	// one request exercises the whole BatchGetServicesResponse shape: an ENABLED
	// entry plus a DISABLED (never-enabled) one, in request order.
	suBase := "/v1/projects/" + p + "/services"
	suEnabled := "conf-su-" + suffix + ".googleapis.com"
	suAbsent := "conf-su-absent-" + suffix + ".googleapis.com"
	sc = append(sc,
		Scenario{Service: "serviceusage", Method: "POST", Path: suBase + "/" + suEnabled + ":enable"},
		Scenario{Service: "serviceusage", Method: "GET", Path: suBase + "/" + suEnabled},
		Scenario{Service: "serviceusage", Method: "GET", Path: suBase + "?filter=state:ENABLED"},
		Scenario{Service: "serviceusage", Method: "GET",
			Path: suBase + ":batchGet?names=" + url.PathEscape("projects/"+p+"/services/"+suEnabled) +
				"&names=" + url.PathEscape("projects/"+p+"/services/"+suAbsent)},
		Scenario{Service: "serviceusage", Method: "POST", Path: suBase + "/" + suEnabled + ":disable"},
	)

	// ─── Managed Kafka + Dataproc Metastore shared LRO operations ─────────────
	// Both services share the bare /v1/projects/{p}/locations/{l}/operations
	// path with Cloud Workflows on one origin, so the recorder addresses them
	// under their own host token (managedkafka.localhost / metastore.localhost):
	// the adapter's operationsHostService routes the operations family to the
	// owning provider there, while the default host keeps it on Workflows. A
	// cluster/service create returns an inline done operation whose name is read
	// back through the owning service's own GetOperation, and the create's
	// operation is listed through its ListOperations.
	kafkaBase := "/v1/projects/" + p + "/locations/us-central1"
	kafkaCluster := "conf-kafka-" + suffix
	kafkaClusterPath := kafkaBase + "/clusters/" + kafkaCluster
	const kafkaTopic = "conf-orders"
	kafkaGroup := "conf-group-" + suffix
	kafkaTopicName := "projects/" + p + "/locations/us-central1/clusters/" + kafkaCluster + "/topics/" + kafkaTopic
	sc = append(sc,
		Scenario{Service: "managedkafka", Method: "POST",
			Path: kafkaBase + "/clusters?clusterId=" + kafkaCluster,
			Save: map[string]string{"kafkaOp": "name"}, Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "GET",
			Path: "/v1/${kafkaOp}", Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "GET",
			Path: kafkaBase + "/operations", Host: "managedkafka.localhost:4588"},
		// A topic to hold the committed offsets, then the ACL lifecycle. ACL
		// create/read are synchronous; the PATCH carries the create's etag
		// (optimistic concurrency) and is the acl.update cell.
		Scenario{Service: "managedkafka", Method: "POST",
			Path: kafkaClusterPath + "/topics?topicId=" + kafkaTopic,
			Body: `{"partitionCount":3,"replicationFactor":1}`,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "POST",
			Path: kafkaClusterPath + "/acls?aclId=topic%2F" + kafkaTopic,
			Body: `{"aclEntries":[{"principal":"User:conformance","permissionType":"ALLOW","operation":"READ","host":"*"}]}`,
			Save: map[string]string{"kafkaAclEtag": "etag"},
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "PATCH",
			Path: kafkaClusterPath + "/acls/topic%2F" + kafkaTopic,
			Body: `{"etag":"${kafkaAclEtag}","aclEntries":[` +
				`{"principal":"User:conformance","permissionType":"ALLOW","operation":"READ","host":"*"},` +
				`{"principal":"User:conformance","permissionType":"DENY","operation":"WRITE","host":"*"}]}`,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "GET",
			Path: kafkaClusterPath + "/acls/topic%2F" + kafkaTopic,
			Host: "managedkafka.localhost:4588"},
		// Consumer groups have no create RPC. In the mock topology (no live
		// broker) the LITERAL GROUP ACL below — the only control-plane
		// reference that names a group — seeds the group record, so the
		// get/update/delete surface is reachable and the group starts empty.
		Scenario{Service: "managedkafka", Method: "POST",
			Path: kafkaClusterPath + "/acls?aclId=consumerGroup%2F" + kafkaGroup,
			Body: `{"aclEntries":[{"principal":"User:conformance","permissionType":"ALLOW","operation":"READ","host":"*"}]}`,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "GET",
			Path: kafkaClusterPath + "/consumerGroups/" + kafkaGroup,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "PATCH",
			Path: kafkaClusterPath + "/consumerGroups/" + kafkaGroup + "?updateMask=topics",
			Body: `{"name":"projects/` + p + `/locations/us-central1/clusters/` + kafkaCluster +
				`/consumerGroups/` + kafkaGroup + `","topics":{"` + kafkaTopicName + `":{"partitions":{"0":{"offset":"1"}}}}}`,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "GET",
			Path: kafkaClusterPath + "/consumerGroups/" + kafkaGroup,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "DELETE",
			Path: kafkaClusterPath + "/consumerGroups/" + kafkaGroup,
			Host: "managedkafka.localhost:4588"},
		Scenario{Service: "managedkafka", Method: "DELETE",
			Path: kafkaClusterPath, Host: "managedkafka.localhost:4588"},
	)

	msBase := "/v1/projects/" + p + "/locations/us-central1"
	msSvc := "conf-ms-" + suffix
	sc = append(sc,
		Scenario{Service: "metastore", Method: "POST",
			Path: msBase + "/services?serviceId=" + msSvc,
			Save: map[string]string{"msOp": "name"}, Host: "metastore.localhost:4588"},
		Scenario{Service: "metastore", Method: "GET",
			Path: "/v1/${msOp}", Host: "metastore.localhost:4588"},
		Scenario{Service: "metastore", Method: "GET",
			Path: msBase + "/operations", Host: "metastore.localhost:4588"},
		Scenario{Service: "metastore", Method: "DELETE",
			Path: msBase + "/services/" + msSvc, Host: "metastore.localhost:4588"},
	)

	// ─── Cloud Workflows management + revision history ────────────────────────
	// A workflow is created (its create LRO response carries the first
	// revisionId), read, its revisions listed, its source updated (minting a
	// second revision), its revisions listed again, the first revision fetched
	// by revisionId, a missing workflow proven NotFound, then deleted.
	wfID := "conf-wf-" + suffix
	wfLocBase := "/v1/projects/" + p + "/locations/us-central1"
	wfBase := wfLocBase + "/workflows"
	wfName := wfBase + "/" + wfID
	sc = append(sc,
		Scenario{Service: "workflows", Method: "POST", Path: wfBase + "?workflowId=" + wfID,
			Body: `{"description":"conformance","sourceContents":"main:\n  steps:\n    - r:\n        return: 1\n"}`,
			Save: map[string]string{"wfRev": "response.revisionId", "wfOp": "name"}},
		// The create's done operation is persisted under the location's
		// operations collection; read it back and delete it (operations.get /
		// operations.delete) before the workflow itself is deleted.
		Scenario{Service: "workflows", Method: "GET", Path: "/v1/${wfOp}"},
		Scenario{Service: "workflows", Method: "GET", Path: wfLocBase + "/operations"},
		Scenario{Service: "workflows", Method: "DELETE", Path: "/v1/${wfOp}"},
		Scenario{Service: "workflows", Method: "GET", Path: wfName},
		Scenario{Service: "workflows", Method: "GET", Path: wfName + ":listRevisions"},
		Scenario{Service: "workflows", Method: "PATCH", Path: wfName + "?updateMask=sourceContents",
			Body: `{"sourceContents":"main:\n  steps:\n    - r:\n        return: 2\n"}`},
		Scenario{Service: "workflows", Method: "GET", Path: wfName + ":listRevisions"},
		Scenario{Service: "workflows", Method: "GET", Path: wfName + "?revisionId=${wfRev}"},
		Scenario{Service: "workflows", Method: "GET", Path: wfBase + "/missing-" + suffix},
		Scenario{Service: "workflows", Method: "DELETE", Path: wfName},
	)

	// ─── Cloud Functions v1 operations ────────────────────────────────────────
	// The vendored service Discovery document enumerates the common
	// google.longrunning operations family on the v1 surface only: operations.get
	// at v1/{+name} (v1 operation names are top-level, operations/{id}) and
	// operations.list at v1/operations with the parent in the documented `name`
	// query parameter. A v1 create captures the persisted operation name; the
	// list reads it back scoped to its location and across the project's
	// locations (locations/-), as the LRO documentation allows.
	fn1ID := "conf-fn1-" + suffix
	fn1Name := "projects/" + p + "/locations/us-central1/functions/" + fn1ID
	sc = append(sc,
		Scenario{Service: "functions", Method: "POST",
			Path: "/v1/projects/" + p + "/locations/us-central1/functions",
			Body: fmt.Sprintf(`{"name":%q,"runtime":"nodejs20","entryPoint":"handler"}`, fn1Name),
			Save: map[string]string{"fn1op": "name"}},
		Scenario{Service: "functions", Method: "GET", Path: "/v1/${fn1op}"},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v1/operations?name=" + url.PathEscape("projects/"+p+"/locations/us-central1")},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v1/operations?name=" + url.PathEscape("projects/"+p+"/locations/-")},
	)

	// ─── Cloud Dataproc job / operation / workflow-template lifecycle ─────────
	// The probe cannot stage these: a job must be submitted to an existing
	// cluster and carries a server-assigned reference.jobId, the cluster create's
	// operation is the only addressable Dataproc LRO, and a workflow template
	// update needs a previous revision (its `version` is the etag). The chain
	// drives the public JSON API only and captures every server-assigned name.
	dpBase := "/v1/projects/" + p + "/regions/us-central1"
	dpCluster := "conf-dp-" + suffix
	dpJob := "conf-dpjob-" + suffix
	dpTemplate := "conf-dpwt-" + suffix
	dpTemplateBody := fmt.Sprintf(
		`{"id":%q,"placement":{"managedCluster":{"clusterName":%q}},"jobs":[{"stepId":"extract","pysparkJob":{"mainPythonFileUri":"gs://conf/%s/extract.py"}}]}`,
		dpTemplate, dpCluster, suffix)
	sc = append(sc,
		// A create must carry exactly one of config or virtualClusterConfig, so
		// the GCE chain sends the Compute-Engine shape (the read fills the rest).
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/clusters",
			Body: fmt.Sprintf(`{"projectId":%q,"clusterName":%q,"config":{"gceClusterConfig":{"zoneUri":"us-central1-a"}}}`, p, dpCluster),
			Save: map[string]string{"dpClusterOp": "name"}},
		Scenario{Service: "dataproc", Method: "GET", Path: "/v1/${dpClusterOp}"},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/clusters/" + dpCluster},
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/jobs:submit",
			Body: fmt.Sprintf(
				`{"job":{"reference":{"jobId":%q},"placement":{"clusterName":%q},"pysparkJob":{"mainPythonFileUri":"gs://conf/%s/extract.py"}}}`,
				dpJob, dpCluster, suffix)},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/jobs/" + dpJob},
		Scenario{Service: "dataproc", Method: "PATCH",
			Path: dpBase + "/jobs/" + dpJob + "?updateMask=labels",
			Body: `{"labels":{"conformance":"true"}}`},
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/jobs/" + dpJob + ":cancel", Body: `{}`},
		// CANCEL_PENDING -> CANCEL_STARTED -> CANCELLED settles one hop per read;
		// only a terminal job may be deleted.
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/jobs/" + dpJob},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/jobs/" + dpJob},
		Scenario{Service: "dataproc", Method: "DELETE", Path: dpBase + "/jobs/" + dpJob},
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/workflowTemplates",
			Body: dpTemplateBody},
		Scenario{Service: "dataproc", Method: "PUT", Path: dpBase + "/workflowTemplates/" + dpTemplate,
			Body: fmt.Sprintf(
				`{"id":%q,"version":1,"labels":{"conformance":"true"},"placement":{"managedCluster":{"clusterName":%q}},"jobs":[{"stepId":"extract","pysparkJob":{"mainPythonFileUri":"gs://conf/%s/extract-v2.py"}}]}`,
				dpTemplate, dpCluster, suffix)},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/workflowTemplates/" + dpTemplate},
		Scenario{Service: "dataproc", Method: "DELETE", Path: dpBase + "/workflowTemplates/" + dpTemplate},
		Scenario{Service: "dataproc", Method: "DELETE", Path: dpBase + "/clusters/" + dpCluster},
	)

	// ─── Dataproc on GKE: cross-API virtual cluster + node pools ──────────────
	// The cross-API chain users actually exercise: a standard,
	// Workload-Identity-enabled GKE cluster is created through container.v1
	// (path-mode prefix, as in the GKE block above), then virtual clusters are
	// created through dataproc.v1 and their pools are read back through the GKE
	// API. Three shapes are recorded: one named-only pool ("any node pool with
	// the specified name is used"), three pools with a shaped/autoscaled one, and
	// an ERROR-status cluster from a nodePoolConfig that disagrees with an
	// existing pool. The patch rejection and the pools-survive-delete read close
	// the lifecycle. nodePoolConfig is input-only, so the virtual-cluster reads
	// carry nodePoolTarget without it.
	dpGkeCluster := "conf-gke-dp-" + suffix
	dpGkeClusterBase := "/container/v1/projects/" + p + "/locations/us-central1/clusters/" + dpGkeCluster
	dpGkeTarget := "projects/" + p + "/locations/us-central1/clusters/" + dpGkeCluster
	dpGkeVC := func(name, namespace string, pools []string, software string) string {
		return fmt.Sprintf(
			`{"projectId":%q,"clusterName":%q,"virtualClusterConfig":{"stagingBucket":%q,"kubernetesClusterConfig":{"kubernetesNamespace":%q,"gkeClusterConfig":{"gkeClusterTarget":%q,"nodePoolTarget":[%s]},"kubernetesSoftwareConfig":{"componentVersion":{"SPARK":%q}}}}}`,
			p, name, "dataproc-staging-"+p, namespace, dpGkeTarget, strings.Join(pools, ","), software)
	}
	// dpGkePool is the full node-pool resource name a nodePoolTarget must carry
	// (Discovery's documented format; a bare id is InvalidArgument, DPG44).
	dpGkePool := func(id string) string { return dpGkeTarget + "/nodePools/" + id }
	dpGkeA := "conf-dpgke-a-" + suffix
	dpGkeB := "conf-dpgke-b-" + suffix
	dpGkeC := "conf-dpgke-c-" + suffix
	dpGkeD := "conf-dpgke-d-" + suffix
	dpGkePoolA := "conf-dp-pool-a-" + suffix
	dpGkeShaped := "conf-dp-shaped-" + suffix
	dpGkeDriver := "conf-dp-driver-" + suffix
	dpGkeExecutor := "conf-dp-executor-" + suffix
	dpGkeTaken := "conf-dp-taken-" + suffix
	dpGkePoolD := "conf-dp-pool-d-" + suffix
	sc = append(sc,
		// Prerequisite GKE cluster.
		Scenario{Service: "container", Method: "POST",
			Path: "/container/v1/projects/" + p + "/locations/us-central1/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":1,"workloadIdentityConfig":{"workloadPool":%q}}}`,
				dpGkeCluster, p+".svc.id.goog")},
		// Virtual cluster A: one named-only pool, created by the reconciler.
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/clusters",
			Body: dpGkeVC(dpGkeA, "conf-ns-a-"+suffix,
				[]string{fmt.Sprintf(`{"nodePool":%q,"roles":["DEFAULT"]}`, dpGkePool(dpGkePoolA))}, "3.5"),
			Save: map[string]string{"dpGkeAOp": "name"}},
		Scenario{Service: "dataproc", Method: "GET", Path: "/v1/${dpGkeAOp}"},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/clusters/" + dpGkeA},
		Scenario{Service: "container", Method: "GET", Path: dpGkeClusterBase + "/nodePools"},
		Scenario{Service: "container", Method: "GET", Path: dpGkeClusterBase + "/nodePools/" + dpGkePoolA},
		// Virtual cluster B: three pools, one caller-shaped with autoscaling and
		// a zone, the driver/executor split across two named-only pools.
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/clusters",
			Body: dpGkeVC(dpGkeB, "conf-ns-b-"+suffix, []string{
				fmt.Sprintf(`{"nodePool":%q,"roles":["DEFAULT","CONTROLLER"],"nodePoolConfig":{"config":{"machineType":"n1-standard-4","localSsdCount":1},"autoscaling":{"minNodeCount":1,"maxNodeCount":4},"locations":["us-central1-a"]}}`, dpGkePool(dpGkeShaped)),
				fmt.Sprintf(`{"nodePool":%q,"roles":["SPARK_DRIVER"]}`, dpGkePool(dpGkeDriver)),
				fmt.Sprintf(`{"nodePool":%q,"roles":["SPARK_EXECUTOR"]}`, dpGkePool(dpGkeExecutor)),
			}, "latest"),
			Save: map[string]string{"dpGkeBOp": "name"}},
		Scenario{Service: "dataproc", Method: "GET", Path: "/v1/${dpGkeBOp}"},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/clusters/" + dpGkeB},
		Scenario{Service: "container", Method: "GET", Path: dpGkeClusterBase + "/nodePools"},
		// Virtual cluster C: an existing pool with a shape the request
		// disagrees with. The create is accepted and the cluster settles in
		// ERROR with a status.detail naming the pool and the field.
		Scenario{Service: "container", Method: "POST", Path: dpGkeClusterBase + "/nodePools",
			Body: fmt.Sprintf(`{"nodePool":{"name":%q,"initialNodeCount":1,"config":{"machineType":"e2-medium"}}}`, dpGkeTaken)},
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/clusters",
			Body: dpGkeVC(dpGkeC, "conf-ns-c-"+suffix, []string{
				fmt.Sprintf(`{"nodePool":%q,"roles":["DEFAULT"],"nodePoolConfig":{"config":{"machineType":"n1-standard-4"}}}`, dpGkePool(dpGkeTaken)),
			}, "3.5"),
			Save: map[string]string{"dpGkeCOp": "name"}},
		Scenario{Service: "dataproc", Method: "GET", Path: "/v1/${dpGkeCOp}"},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/clusters/" + dpGkeC},
		// Virtual cluster D: the `dataproc-2.2` image-line alias. It is the only
		// image line real GCP accepts as a Spark version for a virtual cluster and
		// it dereferences to the newest engine release, exactly like `latest`
		// (recorded from real GCP, DPG36).
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/clusters",
			Body: dpGkeVC(dpGkeD, "conf-ns-d-"+suffix,
				[]string{fmt.Sprintf(`{"nodePool":%q,"roles":["DEFAULT"]}`, dpGkePool(dpGkePoolD))}, "dataproc-2.2"),
			Save: map[string]string{"dpGkeDOp": "name"}},
		Scenario{Service: "dataproc", Method: "GET", Path: "/v1/${dpGkeDOp}"},
		Scenario{Service: "dataproc", Method: "GET", Path: dpBase + "/clusters/" + dpGkeD},
		Scenario{Service: "dataproc", Method: "DELETE", Path: dpBase + "/clusters/" + dpGkeD},
		// A nodePoolTarget.nodePool must be the full node-pool resource name:
		// real GCP rejects a bare pool id with the documented pattern (DPG44).
		Scenario{Service: "dataproc", Method: "POST", Path: dpBase + "/clusters",
			Body: dpGkeVC("conf-dpgke-e-"+suffix, "conf-ns-e-"+suffix,
				[]string{fmt.Sprintf(`{"nodePool":%q,"roles":["DEFAULT"]}`, "conf-dp-bare-"+suffix)}, "3.5")},
		// A Dataproc-on-GKE virtual cluster is recreate-only: a patch that would
		// apply virtualClusterConfig is InvalidArgument.
		Scenario{Service: "dataproc", Method: "PATCH",
			Path: dpBase + "/clusters/" + dpGkeA + "?updateMask=virtualClusterConfig",
			Body: dpGkeVC(dpGkeA, "conf-ns-a-"+suffix,
				[]string{fmt.Sprintf(`{"nodePool":%q,"roles":["DEFAULT"]}`, dpGkePool(dpGkePoolA))}, "3.5")},
		// Deletes: the virtual clusters go, their pools stay (real GCP keeps
		// them), so the pool read after the delete is the proof.
		Scenario{Service: "dataproc", Method: "DELETE", Path: dpBase + "/clusters/" + dpGkeA,
			Save: map[string]string{"dpGkeADelOp": "name"}},
		Scenario{Service: "dataproc", Method: "GET", Path: "/v1/${dpGkeADelOp}"},
		Scenario{Service: "dataproc", Method: "DELETE", Path: dpBase + "/clusters/" + dpGkeB},
		Scenario{Service: "container", Method: "GET", Path: dpGkeClusterBase + "/nodePools/" + dpGkePoolA},
		Scenario{Service: "container", Method: "GET", Path: dpGkeClusterBase + "/nodePools/" + dpGkeShaped},
		Scenario{Service: "container", Method: "DELETE", Path: dpGkeClusterBase},
	)

	// ─── Firestore control plane: composite index, runQuery, backups ──────────
	// The probe cannot stage these either: a composite index needs two fields and
	// returns its id inside the create operation's response; runQuery needs a
	// seeded document; and a backup is read-only, produced by a backup schedule.
	// The chain creates a database, drives index create→get→delete, seeds a
	// document and queries it, then creates a daily backup schedule and advances
	// the emulator clock past the first occurrence so the schedule materializes a
	// backup record that is read and deleted.
	fsDB := "conf-db-" + suffix
	fsBase := "/v1/projects/" + p + "/databases/" + fsDB
	fsDoc := "projects/" + p + "/databases/" + fsDB + "/documents/conf-orders/order-1"
	sc = append(sc,
		Scenario{Service: "firestore", Method: "POST",
			Path: "/v1/projects/" + p + "/databases?databaseId=" + fsDB,
			Body: `{"type":"FIRESTORE_NATIVE","locationId":"us-central1"}`},
		Scenario{Service: "firestore", Method: "POST",
			Path: fsBase + "/collectionGroups/conf-orders/indexes",
			Body: `{"queryScope":"COLLECTION","fields":[{"fieldPath":"status","order":"ASCENDING"},{"fieldPath":"createdAt","order":"DESCENDING"}]}`,
			Save: map[string]string{"fsIndex": "response.name"}},
		Scenario{Service: "firestore", Method: "GET", Path: "/v1/${fsIndex}"},
		Scenario{Service: "firestore", Method: "DELETE", Path: "/v1/${fsIndex}"},
		Scenario{Service: "firestore", Method: "POST", Path: fsBase + "/documents:commit",
			Body: fmt.Sprintf(
				`{"writes":[{"update":{"name":%q,"fields":{"status":{"stringValue":"open"},"total":{"integerValue":"42"}}}}]}`,
				fsDoc)},
		Scenario{Service: "firestore", Method: "POST", Path: fsBase + "/documents:runQuery",
			Body: `{"structuredQuery":{"from":[{"collectionId":"conf-orders"}]}}`},
		Scenario{Service: "firestore", Method: "POST", Path: fsBase + "/backupSchedules",
			Body: `{"retention":"86400s","dailyRecurrence":{}}`},
		// A daily schedule's first occurrence is one interval after its create
		// time; the lazy materialization runs on the backup read, so cross it.
		Scenario{Service: "firestore", Method: "GET",
			Path:         "/v1/projects/" + p + "/locations/us-central1/backups",
			AdvanceClock: "25h",
			Save:         map[string]string{"fsBackup": "backups.0.name"}},
		Scenario{Service: "firestore", Method: "GET", Path: "/v1/${fsBackup}"},
		Scenario{Service: "firestore", Method: "DELETE", Path: "/v1/${fsBackup}"},
	)

	// ─── Cloud Logging: folder-scoped views and their IAM policy ──────────────
	// The view IAM trio exists on the folders scope as well as projects; a
	// folder-scoped bucket + view drives get/set/test on the same core.
	logFolder := "folders/123456789012"
	logFolderBucket := "conf-folder-bucket-" + suffix
	logFolderView := logFolderBucket + "/views/conf-folder-view"
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST",
			Path: "/v2/" + logFolder + "/locations/global/buckets?bucketId=" + logFolderBucket,
			Body: `{"description":"conformance folder bucket"}`},
		Scenario{Service: "logging", Method: "POST",
			Path: "/v2/" + logFolder + "/locations/global/buckets/" + logFolderBucket + "/views?viewId=conf-folder-view",
			Body: `{"description":"conformance folder view"}`},
		Scenario{Service: "logging", Method: "POST",
			Path: "/v2/" + logFolder + "/locations/global/buckets/" + logFolderView + ":getIamPolicy",
			Body: `{}`},
		Scenario{Service: "logging", Method: "POST",
			Path: "/v2/" + logFolder + "/locations/global/buckets/" + logFolderView + ":setIamPolicy",
			Body: `{"policy":{"bindings":[{"role":"roles/logging.viewAccessor","members":["user:conformance@example.com"]}]}}`},
		Scenario{Service: "logging", Method: "POST",
			Path: "/v2/" + logFolder + "/locations/global/buckets/" + logFolderView + ":testIamPermissions",
			Body: `{"permissions":["logging.views.get","logging.views.access"]}`},
	)

	// ─── Eventarc: persisted long-running operations ──────────────────────────
	// A trigger create returns a done operation; the core persists it, so the
	// shared operations surface can read, list and delete it. The locations
	// operations path is host-discriminated (it is shared with Workflows), so
	// those three requests carry the eventarc host token.
	eaBase := "/v1/projects/" + p + "/locations/us-central1"
	eaTrigger := "conf-ea-" + suffix
	sc = append(sc,
		Scenario{Service: "eventarc", Method: "POST", Path: eaBase + "/triggers?triggerId=" + eaTrigger,
			Body: `{"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],` +
				`"destination":{"cloudRun":{"service":"conf-run","region":"us-central1"}}}`,
			Save: map[string]string{"eaOp": "name"}},
		Scenario{Service: "eventarc", Method: "GET", Path: "/v1/${eaOp}", Host: "eventarc.localhost:4588"},
		Scenario{Service: "eventarc", Method: "GET", Path: eaBase + "/operations", Host: "eventarc.localhost:4588"},
		Scenario{Service: "eventarc", Method: "POST", Path: "/v1/${eaOp}:cancel", Host: "eventarc.localhost:4588"},
		Scenario{Service: "eventarc", Method: "DELETE", Path: "/v1/${eaOp}", Host: "eventarc.localhost:4588"},
		Scenario{Service: "eventarc", Method: "DELETE", Path: eaBase + "/triggers/" + eaTrigger},
	)

	// ─── Workflow Executions: get + cancel ────────────────────────────────────
	// Executions run synchronously, so the create returns a terminal execution;
	// get reads it back and cancel is the documented no-op on a terminal
	// execution, returning it unchanged.
	exWf := "conf-exec-" + suffix
	exBase := wfLocBase + "/workflows/" + exWf
	sc = append(sc,
		Scenario{Service: "workflows", Method: "POST", Path: wfLocBase + "/workflows?workflowId=" + exWf,
			Body: `{"sourceContents":"main:\n  steps:\n    - r:\n        return: 1\n"}`,
			Save: map[string]string{"exOp": "name"}},
		Scenario{Service: "workflowexecutions", Method: "POST", Path: exBase + "/executions",
			Body: `{"argument":"{}"}`,
			Save: map[string]string{"execName": "name"}},
		Scenario{Service: "workflowexecutions", Method: "GET", Path: "/v1/${execName}?view=FULL"},
		Scenario{Service: "workflowexecutions", Method: "POST", Path: "/v1/${execName}:cancel"},
		Scenario{Service: "workflows", Method: "DELETE", Path: exBase},
		Scenario{Service: "workflows", Method: "DELETE", Path: "/v1/${exOp}"},
	)

	// ─── Cloud Scheduler: pause → resume ──────────────────────────────────────
	// The resume transition requires the paused state, so the chain pauses the
	// job first (the create honours the caller-specified body name).
	schedBase := "/v1/projects/" + p + "/locations/us-central1/jobs"
	schedName := "projects/" + p + "/locations/us-central1/jobs/conf-sched-" + suffix
	sc = append(sc,
		Scenario{Service: "scheduler", Method: "POST", Path: schedBase,
			Body: fmt.Sprintf(`{"name":%q,"schedule":"*/5 * * * *","timeZone":"UTC","httpTarget":{"uri":"http://example.com"}}`, schedName)},
		Scenario{Service: "scheduler", Method: "POST", Path: "/v1/" + schedName + ":pause"},
		Scenario{Service: "scheduler", Method: "POST", Path: "/v1/" + schedName + ":resume"},
		Scenario{Service: "scheduler", Method: "DELETE", Path: "/v1/" + schedName},
	)

	// ─── Cloud Tasks: pause → resume ──────────────────────────────────────────
	// Cloud Tasks names the queue in the body (there is no queueId parameter).
	tasksBase := "/v2/projects/" + p + "/locations/us-central1/queues"
	tasksName := "projects/" + p + "/locations/us-central1/queues/conf-queue-" + suffix
	sc = append(sc,
		Scenario{Service: "tasks", Method: "POST", Path: tasksBase,
			Body: fmt.Sprintf(`{"name":%q}`, tasksName)},
		Scenario{Service: "tasks", Method: "POST", Path: "/v2/" + tasksName + ":pause"},
		Scenario{Service: "tasks", Method: "POST", Path: "/v2/" + tasksName + ":resume"},
		Scenario{Service: "tasks", Method: "DELETE", Path: "/v2/" + tasksName},
	)

	return sc
}

// expandVars substitutes ${name} references in s using vars.
func expandVars(s string, vars map[string]string) string {
	for name, val := range vars {
		s = strings.ReplaceAll(s, "${"+name+"}", val)
	}
	return s
}

// captureVar walks a dotted path into a decoded JSON value and returns its
// scalar string form.
func captureVar(v any, path string) (string, bool) {
	cur := v
	for _, seg := range strings.Split(path, ".") {
		if list, ok := cur.([]any); ok {
			// A numeric segment indexes a JSON array (the first item of a list
			// response, e.g. "backups.0.name").
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(list) {
				return "", false
			}
			cur = list[idx]
			continue
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = m[seg]
		if !ok {
			return "", false
		}
	}
	switch t := cur.(type) {
	case string:
		return t, true
	case bool:
		return fmt.Sprintf("%t", t), true
	case float64:
		return fmt.Sprintf("%v", t), true
	default:
		b, err := json.Marshal(cur)
		if err != nil {
			return "", false
		}
		return string(b), true
	}
}

// loadParquetFixture builds a small Parquet file whose embedded schema the
// emulator derives for a PARQUET gs:// load (BQL3).
func loadParquetFixture() []byte {
	type row struct {
		ID   int64   `parquet:"id"`
		Name string  `parquet:"name"`
		Amt  float64 `parquet:"amt"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	if _, err := w.Write([]row{{1, "alice", 1.5}, {2, "bob", 2.5}}); err != nil {
		panic("parquet fixture write: " + err.Error())
	}
	if err := w.Close(); err != nil {
		panic("parquet fixture close: " + err.Error())
	}
	return buf.Bytes()
}

// loadAvroFixture builds a small Avro Object Container File (BQL3).
func loadAvroFixture() []byte {
	const schema = `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"name","type":"string"},
	 {"name":"amt","type":"double"}
	]}`
	type row struct {
		ID   int64   `avro:"id"`
		Name string  `avro:"name"`
		Amt  float64 `avro:"amt"`
	}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf)
	if err != nil {
		panic("avro fixture encoder: " + err.Error())
	}
	for _, r := range []row{{1, "alice", 1.5}, {2, "bob", 2.5}} {
		if err := enc.Encode(r); err != nil {
			panic("avro fixture encode: " + err.Error())
		}
	}
	if err := enc.Close(); err != nil {
		panic("avro fixture close: " + err.Error())
	}
	return buf.Bytes()
}
