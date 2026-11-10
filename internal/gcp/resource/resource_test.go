package resource

import "testing"

func TestResourceIDFormatters(t *testing.T) {
	r := ResourceID("proj")
	cases := []struct {
		typ, name, want string
	}{
		{"gcs-bucket", "bkt", "bkt"},
		{"gcs-object", "obj", "obj"},
		{"gcs-bucket-policy", "bkt", "projects/_/buckets/bkt"},
		{"gcs-object-policy", "bkt/objects/o", "projects/_/buckets/bkt/objects/o"},
		{"pubsub-topic", "t", "projects/proj/topics/t"},
		{"pubsub-subscription", "s", "projects/proj/subscriptions/s"},
		{"secret", "sec", "projects/proj/secrets/sec"},
		{"kms-keyring", "us/keyring1", "projects/proj/locations/us/keyRings/keyring1"},
		{"kms-cryptokey", "us/keyring1/key1", "projects/proj/locations/us/keyRings/keyring1/cryptoKeys/key1"},
		{"kms-cryptokey-version", "us/keyring1/key1/3", "projects/proj/locations/us/keyRings/keyring1/cryptoKeys/key1/cryptoKeyVersions/3"},
		{"service-account", "sa@x.iam.gserviceaccount.com", "projects/proj/serviceAccounts/sa@x.iam.gserviceaccount.com"},
		{"cloud-function", "us-central1/fn", "projects/proj/locations/us-central1/functions/fn"},
		{"cloud-function", "fn", "projects/proj/locations/-/functions/fn"},
		{"managedkafka-cluster", "us-central1/c1", "projects/proj/locations/us-central1/clusters/c1"},
		{"managedkafka-topic", "us-central1/c1/t1", "projects/proj/locations/us-central1/clusters/c1/topics/t1"},
		{"managedkafka-operation", "us-central1/op1", "projects/proj/locations/us-central1/operations/op1"},
		{"compute-instance", "us-central1-a/vm", "projects/proj/zones/us-central1-a/instances/vm"},
		{"compute-disk", "us-central1-a/d", "projects/proj/zones/us-central1-a/disks/d"},
		{"compute-disk-type", "us-central1-a/pd-ssd", "projects/proj/zones/us-central1-a/diskTypes/pd-ssd"},
		{"compute-machine-type", "us-central1-a/e2-micro", "projects/proj/zones/us-central1-a/machineTypes/e2-micro"},
		{"compute-network", "default", "projects/proj/global/networks/default"},
		{"compute-firewall", "allow-ssh", "projects/proj/global/firewalls/allow-ssh"},
		{"compute-subnetwork", "us-central1/sub", "projects/proj/regions/us-central1/subnetworks/sub"},
		{"compute-image", "debian-12-bookworm-v20241009", "projects/proj/global/images/debian-12-bookworm-v20241009"},
		{"compute-license", "debian-12", "projects/proj/global/licenses/debian-12"},
		{"compute-project", "proj", "projects/proj"},
		// The project formatter names the project it is given, not the scope
		// the resolver is bound to.
		{"compute-project", "other", "projects/other"},
		{"compute-zone", "us-central1-a", "projects/proj/zones/us-central1-a"},
		{"compute-region", "us-central1", "projects/proj/regions/us-central1"}, {"compute-zone-operation", "us-central1-a/op1", "projects/proj/zones/us-central1-a/operations/op1"},
		{"compute-region-operation", "us-central1/op1", "projects/proj/regions/us-central1/operations/op1"},
		{"compute-global-operation", "op1", "projects/proj/global/operations/op1"},
	}
	for _, tc := range cases {
		if got := r(tc.typ, tc.name); got != tc.want {
			t.Errorf("ResourceID(%q, %q) = %q, want %q", tc.typ, tc.name, got, tc.want)
		}
	}

	// Unknown types fall back to the name as-is.
	if got := r("unknown-type", "x"); got != "x" {
		t.Errorf("unknown type = %q, want %q", got, "x")
	}
}

func TestLocRingKeyOf(t *testing.T) {
	if got := locOf("us/keyring1/key1"); got != "us" {
		t.Errorf("locOf = %q, want us", got)
	}
	if got := locOf("no-slash"); got != "global" {
		t.Errorf("locOf(no-slash) = %q, want global", got)
	}
	if got := ringOf("us/keyring1/key1"); got != "keyring1" {
		t.Errorf("ringOf = %q, want keyring1", got)
	}
	if got := ringOf("us/keyring1"); got != "keyring1" {
		t.Errorf("ringOf(no key) = %q, want keyring1", got)
	}
	if got := ringOf("noslash"); got != "noslash" {
		t.Errorf("ringOf(noslash) = %q, want noslash", got)
	}
	if got := keyOf("us/keyring1/key1"); got != "key1" {
		t.Errorf("keyOf = %q, want key1", got)
	}
	if got := keyOf("us/keyring1"); got != "" {
		t.Errorf("keyOf(no key) = %q, want empty", got)
	}
	if got := keyOf("noslash"); got != "" {
		t.Errorf("keyOf(noslash) = %q, want empty", got)
	}
}

// TestIsProjectNumber pins the all-digit rule the Service Usage surface uses to
// tell a project number from a project id: project ids start with a letter, so
// an all-digit reference is always a number (no length cap — an over-long
// number is still not an id).
func TestIsProjectNumber(t *testing.T) {
	for _, ref := range []string{"0", "00000000000", "123456789012", "12345678901234567890"} {
		if !IsProjectNumber(ref) {
			t.Errorf("IsProjectNumber(%q) = false, want true", ref)
		}
	}
	for _, ref := range []string{"", "my-project", "12a4", "-", " 123"} {
		if IsProjectNumber(ref) {
			t.Errorf("IsProjectNumber(%q) = true, want false", ref)
		}
	}
}

// TestProjectNumberFor pins the project-number rule shared by Compute and IAM:
// a numeric project reference is the number itself, while a project id gets the
// synthesized stable value.
func TestProjectNumberFor(t *testing.T) {
	if got, want := ProjectNumberFor("123456789012"), "123456789012"; got != want {
		t.Errorf("ProjectNumberFor(numeric) = %q, want %q", got, want)
	}
	if got, want := ProjectNumberFor("my-project"), ProjectNumber("my-project"); got != want {
		t.Errorf("ProjectNumberFor(id) = %q, want the synthesized %q", got, want)
	}
	if got := ProjectNumberFor(""); got != ProjectNumber("") {
		t.Errorf("ProjectNumberFor(\"\") = %q, want the synthesized value", got)
	}
	if got, want := ProjectNumberFor("12345678901234567890"), ProjectNumber("12345678901234567890"); got != want {
		t.Errorf("ProjectNumberFor(20 digits) = %q, want the synthesized %q", got, want)
	}
	if got, want := ProjectNumberFor("12a4"), ProjectNumber("12a4"); got != want {
		t.Errorf("ProjectNumberFor(non-numeric) = %q, want the synthesized %q", got, want)
	}
}
