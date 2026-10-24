//go:build gcp_conformance

package gcpconformance

import (
	"strings"
	"testing"
)

// TestSynthProbesMatchResolvedMethods is the offline invariant behind the
// coverage gate: every registry operation that resolves to a Discovery method
// must have at least one synthesized probe whose method+path matches that
// Discovery method. Whether the emulator then returns 2xx is the recorder's
// job; this proves the synthesizer emits a probe for every mapped op.
func TestSynthProbesMatchResolvedMethods(t *testing.T) {
	docs := loadDocs(t)
	ix := BuildMethodIndex(docs)
	byService := resolveOps(docs)

	scenarios := SynthScenarios(docs, "000000000000")
	bySvc := map[string][]Scenario{}
	for _, sc := range scenarios {
		bySvc[sc.Service] = append(bySvc[sc.Service], sc)
	}

	var missing []string
	for svc, ops := range byService {
		for _, ro := range ops {
			matched := false
			for _, sc := range bySvc[svc] {
				if !strings.EqualFold(sc.Method, ro.Method.HTTPMethod) {
					continue
				}
				_, m, ok := ix.MatchMethodInService(svc, sc.Method, sc.Path)
				if ok && m.ID == ro.ID {
					matched = true
					break
				}
			}
			if !matched {
				missing = append(missing, ro.Op.Key()+" ["+ro.ID+"]")
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("synthScenarios emitted no probe matching %d mapped method(s):\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// TestSynthCreateAndSingletonShareID proves a synthesized create and the
// singleton probe that reads the resource use the same id, so the recorded
// create actually stages the resource the later probe needs. It checks the
// eventarc channels pair: at least one created channel id must equal the id of
// a channel get probe.
func TestSynthCreateAndSingletonShareID(t *testing.T) {
	docs := loadDocs(t)
	scenarios := SynthScenarios(docs, "000000000000")

	created := map[string]bool{}
	read := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service != "eventarc" {
			continue
		}
		base := strings.SplitN(sc.Path, "?", 2)[0]
		if sc.Method == "POST" && strings.HasSuffix(base, "/channels") {
			q := strings.SplitN(sc.Path, "?", 2)
			if len(q) == 2 {
				for _, kv := range strings.Split(q[1], "&") {
					if v, ok := strings.CutPrefix(kv, "channelId="); ok {
						created[v] = true
					}
				}
			}
		}
		if sc.Method == "GET" && strings.Contains(base, "/channels/") && !strings.Contains(base, ":") {
			parts := strings.Split(base, "/channels/")
			read[parts[len(parts)-1]] = true
		}
	}
	if len(created) == 0 || len(read) == 0 {
		t.Fatalf("expected eventarc channel create+get probes, got %d created, %d read", len(created), len(read))
	}
	for id := range read {
		if created[id] {
			return
		}
	}
	t.Fatalf("no channel get probe reads a created channel id; created=%v read=%v", keys(created), keys(read))
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestPlaceholderOverrides checks collection ids that must be real catalogue
// names rather than generated ones.
func TestPlaceholderOverrides(t *testing.T) {
	cfg := synthConfigFor("compute")
	cases := map[string]string{
		"machineTypes": "e2-medium",
		"providers":    "pubsub.googleapis.com",
		"versions":     "1",
		"acls":         "allTopics",
	}
	for coll, want := range cases {
		if got := placeholderValue(strings.TrimSuffix(coll, "s")+"Id", coll, cfg, "s", nil); got != want {
			t.Errorf("placeholderValue(%s) = %q, want %q", coll, got, want)
		}
	}
}

// TestActionOverridesResolve guards the resolver's override table: a key that
// is not an enumerated operation, or a value that is not a method id in the
// operation's Discovery snapshot, silently removes the op from the coverage
// gate (Resolve returns false, and the gate treats unmapped ops as out of
// scope). Both are regressions this test catches.
func TestActionOverridesResolve(t *testing.T) {
	docs := loadDocs(t)
	enumerated := map[string]bool{}
	for _, op := range Enumerate() {
		enumerated[op.Key()] = true
	}
	idsByService := map[string]map[string]bool{}
	for svc, doc := range docs {
		m := map[string]bool{}
		doc.WalkMethods(func(mm *Method) {
			if mm.ID != "" {
				m[mm.ID] = true
			}
		})
		idsByService[svc] = m
	}
	for key, want := range actionOverrides {
		if !enumerated[key] {
			t.Errorf("actionOverrides key %q is not an enumerated operation", key)
			continue
		}
		i := strings.IndexByte(key, '.')
		svc := providerPrefixes[key[:i]]
		if svc == "" {
			t.Errorf("actionOverrides key %q has no providerPrefixes entry", key)
			continue
		}
		if !idsByService[svc][want] {
			t.Errorf("actionOverrides[%q] = %q is not a method in the %q snapshot", key, want, svc)
		}
	}
}

// TestSynthPrelude verifies the fixed-name prelude resources the body overrides
// reference exist.
func TestSynthPrelude(t *testing.T) {
	pre := SynthPrelude()
	if len(pre) == 0 {
		t.Fatal("empty prelude")
	}
	var haveTopic, haveBus, haveChannel bool
	for _, sc := range pre {
		switch {
		case strings.Contains(sc.Path, "/topics/conf-probe-topic"):
			haveTopic = true
		case strings.Contains(sc.Path, "messageBuses"):
			haveBus = true
		case strings.Contains(sc.Path, "/channels"):
			haveChannel = true
		}
	}
	if !haveTopic || !haveBus || !haveChannel {
		t.Fatalf("prelude missing resources: topic=%v bus=%v channel=%v", haveTopic, haveBus, haveChannel)
	}
}

// TestSynthValidInputs pins the AUD5-2 fixes: the synthesizer must emit inputs
// the emulator accepts (and real GCP would accept), so a probe rejection is a
// real gap, not an artifact.
//
//   - Firestore createDocument: the flatPath collapses the recursive
//     "{+parent=…/documents/**}" binding to one "{documentsId}" segment, which
//     yields a document-shaped path the codec rejects. The probe must emit the
//     real root-collection shape "…/documents/{collectionId}?documentId=" and
//     still resolve to createDocument.
//   - update_mask: Logging/Monitoring resources have no "labels" field, so the
//     generic mask is rejected; the probe must send a real writable field.
func TestSynthValidInputs(t *testing.T) {
	docs := loadDocs(t)
	ix := BuildMethodIndex(docs)
	scenarios := SynthScenarios(docs, "000000000000")

	base := func(p string) string { return strings.SplitN(p, "?", 2)[0] }

	// (a) Firestore createDocument probes.
	created := map[string]bool{}
	var createSeen, getSeen bool
	for _, s := range scenarios {
		if s.Service != "firestore" {
			continue
		}
		b := base(s.Path)
		q := ""
		if i := strings.IndexByte(s.Path, '?'); i >= 0 {
			q = s.Path[i+1:]
		}
		if s.Method == "POST" && strings.Contains(q, "documentId=") {
			_, m, ok := ix.MatchMethodInService("firestore", "POST", s.Path)
			if !ok || m.ID != "firestore.projects.databases.documents.createDocument" {
				t.Errorf("firestore create probe %q resolved to %v (ok=%v), want createDocument", s.Path, m, ok)
				continue
			}
			createSeen = true
			// Real createDocument always has an odd number of segments after
			// the "documents" literal (the recursive parent is a document path).
			if n := segmentsAfterDocumentsProd(b); n == 0 || n%2 == 0 {
				t.Errorf("firestore create probe %q has %d segment(s) after documents, want odd", b, n)
			}
			for _, kv := range strings.Split(q, "&") {
				if id, ok := strings.CutPrefix(kv, "documentId="); ok {
					created[b+"/"+id] = true
				}
			}
		}
		if s.Method == "GET" && strings.Contains(b, "/documents/") {
			if created[b] {
				getSeen = true
			}
		}
	}
	if !createSeen {
		t.Error("no firestore createDocument probe emitted")
	}
	if !getSeen {
		t.Error("no firestore document GET probe read a staged create's document")
	}

	// (b) update_mask: Logging and Monitoring resources with no "labels" field
	// must use a real writable field instead of the rejected generic default.
	var loggingSeen, monitoringSeen bool
	for _, s := range scenarios {
		switch s.Service {
		case "logging":
			if !strings.Contains(s.Path, "updateMask=") {
				continue
			}
			loggingSeen = true
			if strings.Contains(s.Path, "updateMask=labels") {
				t.Errorf("logging probe %q still sends the rejected updateMask=labels", s.Path)
			}
		case "monitoring":
			if !strings.Contains(s.Path, "/services/") || !strings.Contains(s.Path, "updateMask=") {
				continue
			}
			monitoringSeen = true
			if !strings.Contains(s.Path, "updateMask=displayName") {
				t.Errorf("monitoring service/SLO probe %q should send updateMask=displayName", s.Path)
			}
		}
	}
	if !loggingSeen {
		t.Error("no logging updateMask probe emitted")
	}
	if !monitoringSeen {
		t.Error("no monitoring service/SLO updateMask probe emitted")
	}
}

// segmentsAfterDocumentsProd mirrors the production codec's count (the
// conformance package has no access to internal/gcp/adapter).
func segmentsAfterDocumentsProd(name string) int {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	for i, p := range parts {
		if p == "documents" {
			return len(parts) - i - 1
		}
	}
	return 0
}
