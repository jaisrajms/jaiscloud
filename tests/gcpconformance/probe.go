//go:build gcp_conformance

package gcpconformance

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// This file derives a synthetic probe request for every emulator operation the
// registry maps to a Discovery method. The curated Scenarios list (scenarios.go)
// remains the source for stateful flows that need real ordering; the probes here
// fill the coverage gaps so every mapped op gets at least one schema-validated
// response.
//
// The probes are Discovery-driven: the HTTP method and flatPath come from the
// vendored document, path placeholders are filled with deterministic values
// (project = the emulator default; location/region/zone from a per-service
// table; resource ids derived from the owning collection segment), the required
// query parameters and body come from the method's declared parameters/schema,
// and each read/mutate probe is preceded by the creates that establish its
// resource so ordering can never delete a resource out from under a later read.

// synthService is the per-service request context the synthesizer needs: the
// URL path base (the segment the Discovery flatPath omits) and the location-ish
// values substituted for {locationsId}/{region}/{zone}.
type synthService struct {
	base     string
	location string
	region   string
	zone     string
	host     string // Host header override for host-discriminated services
}

// withDefaults fills the unset location/region/zone on a service entry.
func (s synthService) withDefaults() synthService {
	if s.location == "" {
		s.location = "us-central1"
	}
	if s.region == "" {
		s.region = "us-central1"
	}
	if s.zone == "" {
		s.zone = "us-central1-a"
	}
	return s
}

// synthServices is the per-service base + location table. Only services whose
// flatPath does not already carry the emulator's URL prefix (or whose default
// location differs) need an entry.
var synthServices = map[string]synthService{
	"storage":   {base: "/storage/v1/"},
	"compute":   {base: "/compute/v1/"},
	"bigquery":  {base: "/bigquery/v2/"},
	"cloudsql":  {base: "/sql/"},
	"container": {base: "/container/"},
	"kms":       {location: "global"},
	"logging":   {location: "global"},
	// Managed Kafka and Dataproc Metastore share the bare locations OPERATIONS
	// path with Cloud Workflows; their canonical hosts disambiguate it.
	"managedkafka": {host: "managedkafka.localhost:4588"},
	"metastore":    {host: "metastore.localhost:4588"},
	// Eventarc likewise owns its operations under its canonical host.
	"eventarc": {host: "eventarc.localhost:4588"},
}

func synthConfigFor(svc string) synthService {
	return synthServices[svc].withDefaults()
}

// bodyOverrides supplies request fields the emulator requires but the vendored
// Discovery schema does not mark required. Keys are Discovery method ids. A
// value of "$ID" is replaced with the probe's target resource id.
var bodyOverrides = map[string]map[string]any{
	"eventarc.projects.locations.triggers.create": {
		"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
		"destination":  map[string]any{"cloudRun": map[string]any{"service": "probe", "region": "us-central1"}},
	},
	"eventarc.projects.locations.enrollments.create": {
		"messageBus": "projects/" + defaultProject + "/locations/us-central1/messageBuses/probe",
	},
	"eventarc.projects.locations.channelConnections.create": {
		"channel": "projects/" + defaultProject + "/locations/us-central1/channels/probe",
	},

	"iam.projects.serviceAccounts.create": {"accountId": "$ID"},

	"cloudscheduler.projects.locations.jobs.create": {
		"name":     "projects/" + defaultProject + "/locations/us-central1/jobs/$ID",
		"schedule": "* * * * *",
		"timeZone": "UTC",
		"httpTarget": map[string]any{
			"uri":        "http://example.com/probe",
			"httpMethod": "GET",
		},
	},

	"logging.billingAccounts.sinks.create": {
		"name":        "$ID",
		"destination": "logging.googleapis.com/projects/" + defaultProject,
		"filter":      "",
	},
	"logging.folders.sinks.create": {
		"name":        "$ID",
		"destination": "logging.googleapis.com/projects/" + defaultProject,
		"filter":      "",
	},
	"logging.organizations.sinks.create": {
		"name":        "$ID",
		"destination": "logging.googleapis.com/projects/" + defaultProject,
		"filter":      "",
	},
	"logging.billingAccounts.exclusions.create": {
		"name":   "$ID",
		"filter": "severity>=ERROR",
	},
	"logging.folders.exclusions.create": {
		"name":   "$ID",
		"filter": "severity>=ERROR",
	},
	"logging.organizations.exclusions.create": {
		"name":   "$ID",
		"filter": "severity>=ERROR",
	},
	"logging.billingAccounts.locations.buckets.links.create": {
		"bigqueryDataset": map[string]any{"datasetId": "probe"},
	},
	"logging.folders.locations.buckets.links.create": {
		"bigqueryDataset": map[string]any{"datasetId": "probe"},
	},
	"logging.folders.locations.logScopes.create": {
		"name":          "$ID",
		"resourceNames": []any{"projects/" + defaultProject},
	},
	"logging.organizations.locations.logScopes.create": {
		"name":          "$ID",
		"resourceNames": []any{"projects/" + defaultProject},
	},

	"managedkafka.projects.locations.clusters.topics.create": {
		"partitionCount":    1,
		"replicationFactor": 1,
	},
	"managedkafka.projects.locations.clusters.acls.create": {
		"aclEntries": []any{map[string]any{
			"principal":      "User:probe@example.com",
			"operation":      "ALL",
			"permissionType": "ALLOW",
			"host":           "*",
		}},
	},

	"monitoring.services.create": {"displayName": "probe", "basicService": map[string]any{"serviceType": "probe", "serviceLabels": map[string]any{"probe": "probe"}}},

	"dns.managedZones.create": {"dnsName": "probe-" + defaultProject + ".example.com."},
	"cloudtasks.projects.locations.queues.tasks.create": {
		"httpRequest": map[string]any{"httpMethod": "GET", "url": "http://example.com/probe"},
	},

	"pubsub.projects.subscriptions.create": {
		"topic": "projects/" + defaultProject + "/topics/conf-probe-topic",
	},

	"workflows.projects.locations.workflows.create": {
		"sourceContents": "main:\n  steps:\n    - r:\n        return: 1\n",
	},

	"dataproc.projects.regions.workflowTemplates.create": {
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "probe"}},
		"jobs":      []any{map[string]any{"stepId": "s1", "hadoopJob": map[string]any{}}},
	},
	"dataproc.projects.regions.jobs.submit": {
		"job": map[string]any{"placement": map[string]any{"clusterName": "probe"}, "hadoopJob": map[string]any{"mainClass": "probe"}},
	},
	"dataproc.projects.regions.jobs.submitAsOperation": {
		"job": map[string]any{"placement": map[string]any{"clusterName": "probe"}, "hadoopJob": map[string]any{"mainClass": "probe"}},
	},

	"cloudresourcemanager.projects.create": {"projectId": "$ID"},
	"serviceusage.services.batchEnable":    {"serviceIds": []any{"serviceusage.googleapis.com"}},
	"iam.projects.serviceAccounts.signJwt": {"payload": "{}"},

	"secretmanager.projects.secrets.addVersion": {"payload": map[string]any{"data": "c2VjcmV0"}},

	"container.projects.locations.clusters.setMaintenancePolicy": {
		"maintenancePolicy": map[string]any{"window": map[string]any{"dailyMaintenanceWindow": map[string]any{"startTime": "03:00"}}},
	},
	"container.projects.locations.clusters.setNetworkPolicy": {
		"networkPolicy": map[string]any{"enabled": true},
	},

	"cloudfunctions.projects.locations.functions.create": {
		"name":    "projects/" + defaultProject + "/locations/us-central1/functions/$ID",
		"runtime": "go121",
	},

	"dataproc.projects.regions.workflowTemplates.update": {
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "probe"}},
		"jobs":      []any{map[string]any{"stepId": "s1", "hadoopJob": map[string]any{}}},
	},
	"dataproc.projects.regions.workflowTemplates.instantiateInline": {
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "probe"}},
		"jobs":      []any{map[string]any{"stepId": "s1", "hadoopJob": map[string]any{}}},
	},

	"managedkafka.projects.locations.clusters.acls.addAclEntry": {
		"principal":      "User:probe@example.com",
		"operation":      "ALL",
		"permissionType": "ALLOW",
		"host":           "*",
		"name":           "allTopics",
	},
	"managedkafka.projects.locations.clusters.acls.removeAclEntry": {
		"principal":      "User:probe@example.com",
		"operation":      "ALL",
		"permissionType": "ALLOW",
		"host":           "*",
		"name":           "allTopics",
	},

	"monitoring.services.serviceLevelObjectives.create": {
		"displayName":           "probe",
		"goal":                  0.9,
		"rollingPeriod":         "86400s",
		"serviceLevelIndicator": map[string]any{"basicSli": map[string]any{"method": []any{"GET"}}},
	},

	"cloudkms.projects.locations.keyRings.importJobs.create": {
		"importMethod":    "RSA_OAEP_3072_SHA1_AES_256",
		"protectionLevel": "SOFTWARE",
	},

	// KMS locations.generateRandomBytes: lengthBytes is documented 8..1024 with
	// no Discovery `required`, so the generic probe would send 0 and be rejected.
	"cloudkms.projects.locations.generateRandomBytes": {"lengthBytes": 8},

	// Memorystore instance custom methods. failover requires a STANDARD_HA tier
	// instance (the create override below makes every synthesized instance HA);
	// import/export require their GCS config; rescheduleMaintenance requires a
	// rescheduleType.
	"redis.projects.locations.instances.create": {"tier": "STANDARD_HA"},
	"redis.projects.locations.instances.import": {
		"inputConfig": map[string]any{"gcsSource": map[string]any{"uri": "gs://conf-probe/import.rdb"}},
	},
	"redis.projects.locations.instances.export": {
		"outputConfig": map[string]any{"gcsDestination": map[string]any{"uri": "gs://conf-probe/export.rdb"}},
	},
	"redis.projects.locations.instances.rescheduleMaintenance": {"rescheduleType": "IMMEDIATE"},

	// Pub/Sub: subscriptions.seek needs exactly one of time/snapshot;
	// modifyPushConfig accepts an empty pushConfig (which clears it).
	"pubsub.projects.subscriptions.seek":             {"time": "2020-01-01T00:00:00Z"},
	"pubsub.projects.subscriptions.modifyPushConfig": {"pushConfig": map[string]any{}},

	// Firestore documents.partitionQuery needs a structured query and a positive
	// partition count. A single partition is a valid request (the emulator, like
	// the SDK contract, answers the single full-range partition with no split).
	"firestore.projects.databases.documents.partitionQuery": {
		"partitionCount":  "1",
		"structuredQuery": map[string]any{"from": []any{map[string]any{"collectionId": "conf-probe"}}},
	},

	// Firestore documents.executePipeline needs a non-empty stage list. A
	// collection source over the (empty) probe collection is a valid pipeline
	// that returns an empty result set rather than the 400 an empty request body
	// produces.
	"firestore.projects.databases.documents.executePipeline": {
		"structuredPipeline": map[string]any{
			"pipeline": map[string]any{
				"stages": []any{
					map[string]any{"name": "collection", "args": []any{map[string]any{"referenceValue": "/conf-probe"}}},
				},
			},
		},
	},
}

// extraSetupServices maps a probe service to other services whose Discovery
// methods may create the resources the probe needs (a cross-service parent).
var extraSetupServices = map[string][]string{
	"workflowexecutions": {"workflows"},
	"iamcredentials":     {"iam"},
}

// createNameField maps a create method to the (dotted) body field its handler
// reads the resource id from, when that field is not "name". The value is set
// to the probe's target id so the following get/patch/delete resolve.
var createNameField = map[string]string{
	"container.projects.locations.clusters.create":           "cluster.name",
	"container.projects.locations.clusters.nodePools.create": "nodePool.name",
	"dataproc.projects.regions.clusters.create":              "clusterName",
	"dataproc.projects.regions.workflowTemplates.create":     "id",
}

// placeholderOverrides pins a collection's probe id to a value the emulator
// requires instead of a generated one (a real catalogue entry, the single
// version of a secret, a valid Kafka ACL id).
var placeholderOverrides = map[string]string{
	"machineTypes": "e2-medium",
	"providers":    "pubsub.googleapis.com",
	"versions":     "1",
	"acls":         "allTopics",
	// Firestore Admin backup schedules have no id on create (the emulator
	// assigns the single schedule id "1"), so a get/patch/delete probe must
	// target "1" rather than a synthesized per-operation id.
	"backupSchedules": "1",
}

// collectionCreateAlias maps a path collection segment to create-like method
// suffixes when the id is not created by a ".create"/".insert" method (a secret
// version is minted by secrets:addVersion).
var collectionCreateAlias = map[string][]string{
	"versions": {".addVersion"},
	// Firestore documents are minted by documents.createDocument, which the
	// ".create"/".insert" suffix match misses, so a document read/patch/delete
	// probe can stage its target.
	"documents": {".createDocument"},
	// Firestore collection-group fields have no create method; a field is
	// materialized by fields.patch (create-or-update), so a field read probe
	// stages its target that way.
	"fields": {".patch"},
}

// resolvedOp is one registry operation joined to the Discovery method it
// resolves to.
type resolvedOp struct {
	Op     Operation
	Method *Method
	ID     string
}

// resolveOps joins every enumerable registry operation to its Discovery method
// (if any), grouped by wire service and sorted for determinism. One entry is
// kept per (service, method): a single probe covers every op that resolves to
// the same method (TranscriptCoverage is keyed by the resolved method).
func resolveOps(docs map[string]*DiscoveryDoc) map[string][]resolvedOp {
	resolver := NewActionResolver(docs)
	methods := map[string]map[string]*Method{}
	for svc, doc := range docs {
		m := map[string]*Method{}
		doc.WalkMethods(func(mm *Method) {
			if mm.ID != "" {
				m[mm.ID] = mm
			}
		})
		methods[svc] = m
	}

	byService := map[string][]resolvedOp{}
	seen := map[string]bool{}
	for _, op := range Enumerate() {
		id, ok := resolver.Resolve(op)
		if !ok {
			continue
		}
		m := methods[op.Service][id]
		if m == nil {
			continue
		}
		key := op.Service + "\x00" + id
		if seen[key] {
			continue
		}
		seen[key] = true
		byService[op.Service] = append(byService[op.Service], resolvedOp{Op: op, Method: m, ID: id})
	}
	for svc := range byService {
		ops := byService[svc]
		sort.Slice(ops, func(i, j int) bool { return ops[i].Op.Key() < ops[j].Op.Key() })
		byService[svc] = ops
	}
	return byService
}

// templateOf returns the flatPath (preferred) or path of a method, without a
// leading slash.
func templateOf(m *Method) string {
	t := m.FlatPath
	if t == "" {
		t = m.Path
	}
	return strings.TrimPrefix(t, "/")
}

// SynthScenarios derives one probe per registry operation that resolves to a
// Discovery method. Each probe that reads or mutates an existing resource is
// preceded by the creates for its ancestor and target collections, using a
// per-operation id token so a delete/disable probe can never affect another
// probe's resource.
func SynthScenarios(docs map[string]*DiscoveryDoc, suffix string) []Scenario {
	byService := resolveOps(docs)

	var out []Scenario
	for _, svc := range sortedServiceKeys(byService) {
		cfg := synthConfigFor(svc)
		doc := docs[svc]
		ops := byService[svc]

		allMethods := map[string]*Method{}
		methodService := map[string]string{}
		registryResolves := map[string]bool{}
		if doc != nil {
			doc.WalkMethods(func(m *Method) {
				if m.ID != "" {
					allMethods[m.ID] = m
					methodService[m.ID] = svc
				}
			})
		}
		// Some services' probes need resources owned by another service's
		// Discovery surface (a Workflow execution needs a Workflow; an
		// IAMCredentials call needs a service account). Fold the extra service's
		// methods in so the shared setup can create them.
		for _, extra := range extraSetupServices[svc] {
			if d := docs[extra]; d != nil {
				d.WalkMethods(func(m *Method) {
					if m.ID != "" {
						allMethods[m.ID] = m
						methodService[m.ID] = extra
					}
				})
			}
		}
		{
			resolver := NewActionResolver(docs)
			want := map[string]bool{svc: true}
			for _, extra := range extraSetupServices[svc] {
				want[extra] = true
			}
			for _, op := range Enumerate() {
				if !want[op.Service] {
					continue
				}
				if id, ok := resolver.Resolve(op); ok {
					registryResolves[id] = true
				}
			}
		}

		for i, ro := range ops {
			token := strconv.Itoa(i)
			tokens := map[string]string{}
			tpl := probeTemplate(ro.Method)

			isCreate := createMethodID(ro.ID, allMethods, registryResolves) != ""
			setup := instanceChain(tpl)
			if isCreate && lastSegmentIsPlaceholder(tpl) && len(setup) > 0 {
				// A path-id create (e.g. Pub/Sub topics.create PUT) creates its
				// own target; only its ancestors need to exist first.
				setup = setup[:len(setup)-1]
			}
			// A method can reference the same owning collection at more than one
			// nesting level (Firestore's documents/{a}/{b}); emit each
			// prerequisite create once, and never stage the target method via
			// itself.
			staged := map[string]bool{}
			for _, coll := range setup {
				tokens[coll] = token
				createID := createForCollection(coll, tpl, allMethods, registryResolves)
				if createID == "" || createID == ro.ID || staged[createID] {
					continue
				}
				staged[createID] = true
				// The prerequisite may belong to another Discovery surface
				// (workflowexecutions -> workflows); record it under its owning
				// service so the harness matches it to the right document.
				createSvc := methodService[createID]
				if createSvc == "" {
					createSvc = svc
				}
				out = append(out, synthScenario(createSvc, synthConfigFor(createSvc), docs[createSvc], allMethods[createID], suffix, tokens))
			}
			out = append(out, synthScenario(svc, cfg, doc, ro.Method, suffix, tokens))
		}
	}
	return out
}

// SynthPrelude creates the fixed-name resources the synthesized body overrides
// reference across collections (a Pub/Sub topic, an Eventarc message bus and a
// channel). Their names are the literals hard-coded in bodyOverrides; the
// recorder runs against an ephemeral emulator, so a run cannot collide with a
// previous one.
func SynthPrelude() []Scenario {
	p := defaultProject
	return []Scenario{
		{Service: "pubsub", Method: "PUT",
			Path: "/v1/projects/" + p + "/topics/conf-probe-topic",
			Body: `{"name":"projects/` + p + `/topics/conf-probe-topic"}`},
		{Service: "eventarc", Method: "POST",
			Path: "/v1/projects/" + p + "/locations/us-central1/messageBuses?messageBusId=probe", Body: `{}`},
		{Service: "eventarc", Method: "POST",
			Path: "/v1/projects/" + p + "/locations/us-central1/channels?channelId=probe", Body: `{}`},
	}
}

// createMethodID returns the Discovery create/insert method id when the method
// is itself a create on a collection endpoint.
func createMethodID(id string, all map[string]*Method, registryResolves map[string]bool) string {
	if !strings.HasSuffix(id, ".create") && !strings.HasSuffix(id, ".insert") {
		return ""
	}
	if all[id] == nil || !registryResolves[id] {
		return ""
	}
	return id
}

// createForCollection returns the create/insert method id for a collection
// segment, if one resolves. A small alias table covers collections whose id is
// minted by a differently named method (a secret version, via :addVersion).
func createForCollection(collection, targetTpl string, all map[string]*Method, registryResolves map[string]bool) string {
	var cands []string
	for _, suf := range []string{".create", ".insert"} {
		for id := range all {
			if strings.HasSuffix(id, "."+collection+suf) && registryResolves[id] {
				cands = append(cands, id)
			}
		}
	}
	// Alias suffixes are matched with their full tail (a secret version is
	// minted by secrets:addVersion, not by a versions.create method).
	for _, suf := range collectionCreateAlias[collection] {
		for id := range all {
			if strings.HasSuffix(id, suf) && registryResolves[id] {
				cands = append(cands, id)
			}
		}
	}
	// Several services declare the same collection under multiple parents
	// (logging's billingAccounts/folders/organizations/projects, container's
	// zones/locations, ...). Pick the candidate whose template shares the
	// longest literal prefix with the target's, so the prerequisite is staged
	// under the same parent and the target read resolves. Sorted so the choice
	// is deterministic across runs.
	sort.Strings(cands)
	best := ""
	bestScore := -1
	for _, id := range cands {
		score := sharedPrefix(templateOf(all[id]), targetTpl)
		if score > bestScore {
			bestScore = score
			best = id
		}
	}
	return best
}

// sharedPrefix counts the leading path segments two flatPath templates share.
func sharedPrefix(a, b string) int {
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	n := 0
	for n < len(as) && n < len(bs) && as[n] == bs[n] {
		n++
	}
	return n
}

// instanceChain returns the resource collection segments (the literal segment
// preceding each resource placeholder) a template references, in order. Infra
// placeholders (project/location/region/zone/organization/folder/billing) are
// excluded.
func instanceChain(tpl string) []string {
	var chain []string
	lastLiteral := ""
	for _, seg := range strings.Split(tpl, "/") {
		name, _, isPH := parsePlaceholder(seg)
		if !isPH {
			lastLiteral = seg
			continue
		}
		if isInfraPlaceholder(name) {
			continue
		}
		coll := lastLiteral
		if coll == "" {
			coll = strings.TrimSuffix(strings.TrimSuffix(name, "Id"), "s")
		}
		chain = append(chain, coll)
	}
	return chain
}

// lastSegmentIsPlaceholder reports whether the template's final segment is a
// {placeholder}.
func lastSegmentIsPlaceholder(tpl string) bool {
	segs := strings.Split(tpl, "/")
	_, _, isPH := parsePlaceholder(segs[len(segs)-1])
	return isPH
}

// isInfraPlaceholder reports whether a placeholder names a fixed scope rather
// than a creatable collection.
func isInfraPlaceholder(name string) bool {
	switch name {
	case "project", "projectsId", "projectId",
		"location", "locationsId", "locationsId1",
		"region", "regionsId", "zone", "zonesId",
		"organizationsId", "foldersId", "billingAccountsId":
		return true
	}
	return isPositionalPrefixPlaceholder(name)
}

// synthScenario builds the probe request for one Discovery method. tokens maps
// a collection segment to the per-operation id token applied to its
// placeholders (and create id parameters) so a create's chosen id and the
// singleton that reads it match.
func synthScenario(svc string, cfg synthService, doc *DiscoveryDoc, m *Method, suffix string, tokens map[string]string) Scenario {
	tpl := probeTemplate(m)
	path := cfg.base + fillTemplate(tpl, cfg, suffix, tokens)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if q := queryParams(svc, m, suffix, tokens); len(q) > 0 {
		path += "?" + q.Encode()
	}

	body := ""
	if m.Request != nil && m.Request.Ref != "" && doc != nil {
		if s, ok := doc.ResolveRef(m.Request.Ref); ok {
			body = synthBody(s, doc, 0)
		}
	}
	targetID := ""
	if isCreateMethodID(m.ID) && !lastSegmentIsPlaceholder(templateOf(m)) {
		targetColl := lastLiteralSegment(templateOf(m))
		targetID = deriveID(targetColl, suffix, tokens[targetColl])
		field := "name"
		if f, ok := createNameField[m.ID]; ok {
			field = f
		}
		body = setJSONField(body, field, targetID)
	}
	if extra, ok := bodyOverrides[m.ID]; ok {
		body = mergeJSONObjects(body, extra)
	}
	body = strings.ReplaceAll(body, "$ID", targetID)

	return Scenario{Service: svc, Method: m.HTTPMethod, Path: path, Body: body, Host: cfg.host}
}

// isCreateMethodID reports whether a Discovery method id is a create/insert.
func isCreateMethodID(id string) bool {
	return strings.HasSuffix(id, ".create") || strings.HasSuffix(id, ".insert")
}

// setJSONField sets a dotted field path in a JSON object body, creating the
// object from scratch when body is empty.
func setJSONField(body, field, value string) string {
	obj := map[string]any{}
	if strings.TrimSpace(body) != "" {
		_ = json.Unmarshal([]byte(body), &obj)
	}
	parts := strings.Split(field, ".")
	cur := obj
	for i, p := range parts {
		if i == len(parts)-1 {
			cur[p] = value
			break
		}
		next, ok := cur[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[p] = next
		}
		cur = next
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return string(b)
}

// mergeJSONObjects merges extra into a JSON object body, preserving base keys
// and adding/overriding with extra.
func mergeJSONObjects(base string, extra map[string]any) string {
	obj := map[string]any{}
	if strings.TrimSpace(base) != "" {
		_ = json.Unmarshal([]byte(base), &obj)
	}
	for k, v := range extra {
		obj[k] = v
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return base
	}
	return string(b)
}

// fillTemplate substitutes {placeholders} in a flatPath with probe values. A
// resource placeholder takes its value from the preceding literal segment (the
// owning collection).
func fillTemplate(tpl string, cfg synthService, suffix string, tokens map[string]string) string {
	segs := strings.Split(tpl, "/")
	lastLiteral := ""
	for i, seg := range segs {
		name, suffixVerb, isPH := parsePlaceholder(seg)
		if !isPH {
			lastLiteral = seg
			continue
		}
		val := placeholderValue(name, lastLiteral, cfg, suffix, tokens)
		segs[i] = val + suffixVerb
	}
	return strings.Join(segs, "/")
}

// parsePlaceholder extracts the placeholder name and any trailing literal
// suffix (e.g. "{triggersId}:getIamPolicy" → ("triggersId", ":getIamPolicy")).
func parsePlaceholder(seg string) (name, suffix string, ok bool) {
	if !strings.HasPrefix(seg, "{") {
		return "", "", false
	}
	j := strings.IndexByte(seg, '}')
	if j < 0 {
		return "", "", false
	}
	return seg[1:j], seg[j+1:], true
}

// placeholderValue returns the deterministic probe value for a path
// placeholder. literal is the preceding path segment (the owning collection).
func placeholderValue(name, literal string, cfg synthService, suffix string, tokens map[string]string) string {
	switch name {
	case "project", "projectsId", "projectId":
		return defaultProject
	case "location", "locationsId", "locationsId1":
		return cfg.location
	case "region", "regionsId":
		return cfg.region
	case "zone", "zonesId":
		return cfg.zone
	case "organizationsId", "foldersId":
		return "123456789012"
	case "billingAccountsId":
		return "012345-6789AB-CDEF01"
	}
	base := literal
	if base == "" {
		base = strings.TrimSuffix(strings.TrimSuffix(name, "Id"), "s")
	}
	// Monitoring (and a few other) Discovery docs split the "projects/{project}"
	// prefix into opaque positional placeholders v1Id/v1Id1 (v3Id/v3Id1, ...).
	if isPositionalPrefixPlaceholder(name) {
		if strings.HasSuffix(name, "Id1") {
			return defaultProject
		}
		return "projects"
	}
	if base == "serviceAccounts" {
		// Service-account lookups use the email, not the bare account id.
		return deriveID(base, suffix, tokens[base]) + "@" + defaultProject + ".iam.gserviceaccount.com"
	}
	if v, ok := placeholderOverrides[base]; ok {
		return v
	}
	return deriveID(base, suffix, tokens[base])
}

// isPositionalPrefixPlaceholder reports whether name is the Discovery-generated
// opaque prefix placeholder v<N>Id / v<N>Id1.
func isPositionalPrefixPlaceholder(name string) bool {
	if len(name) < 4 || name[0] != 'v' {
		return false
	}
	i := 1
	for i < len(name) && name[i] >= '0' && name[i] <= '9' {
		i++
	}
	if i == 1 || !strings.HasPrefix(name[i:], "Id") {
		return false
	}
	rest := name[i+2:]
	return rest == "" || rest == "1"
}

// requestUUID returns a stable RFC 4122 UUID for a probe's requestId parameter.
// The seed (method id + owning collection + per-operation token) is unique per
// synthesized request, so no probe replays another's operation and a probe's
// setup-create never shares the token with the mutation it stages.
func requestUUID(methodID, collection, suffix, token string) string {
	sum := md5.Sum([]byte(methodID + "\x00" + collection + "\x00" + suffix + "\x00" + token))
	sum[6] = (sum[6] & 0x0f) | 0x40 // RFC 4122 version 4
	sum[8] = (sum[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

// deriveID builds a valid, deterministic resource id from the owning collection
// segment plus the per-operation token.
func deriveID(collection, suffix, token string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(collection) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	v := b.String()
	if v == "" {
		v = "probe"
	}
	if len(v) > 8 {
		v = v[:8]
	}
	return "conf" + v + suffix + token
}

// queryParams assembles the query string for a probe: the id-typed parameter a
// create uses (derived from the collection segment so it matches the singleton
// path value) plus any required parameter.
func queryParams(svc string, m *Method, suffix string, tokens map[string]string) urlValues {
	q := urlValues{}
	lastLiteral := lastLiteralSegment(probeTemplate(m))
	names := make([]string, 0, len(m.Parameters))
	for n := range m.Parameters {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := m.Parameters[n]
		if p == nil || p.Location != "query" {
			continue
		}
		if n == "requestId" {
			// The Discovery schema types requestId as an idempotency token —
			// a UUID that replays the original operation when repeated — so a
			// synthetic non-UUID token would be rejected on input, and sharing
			// one token across a probe's setup-create and its own mutation
			// would make the mutation replay the create. Derive a UUID unique
			// to this request (method + target + token).
			q.Set(n, requestUUID(m.ID, lastLiteral, suffix, tokens[lastLiteral]))
			continue
		}
		if strings.HasSuffix(n, "Id") {
			if v, ok := placeholderOverrides[lastLiteral]; ok {
				q.Set(n, v)
			} else {
				q.Set(n, deriveID(lastLiteral, suffix, tokens[lastLiteral]))
			}
			continue
		}
		if strings.EqualFold(n, "updateMask") || strings.EqualFold(n, "fieldMask") {
			q.Set(n, updateMaskValue(svc, m))
			continue
		}
		if p.Required {
			q.Set(n, requiredQueryValue(n))
		}
	}
	return q
}

// updateMaskValue returns the update_mask/field_mask value a probe sends. The
// generic default is "labels", which most resources accept, but a resource
// whose field set has no "labels" rejects it (the merge handlers answer 400
// INVALID_ARGUMENT for a path that names no field, per AIP-134/AIP-161).
// maskFieldByResource pins a real writable field for those resources so the
// probe exercises the merge instead of being rejected on input.
func updateMaskValue(svc string, m *Method) string {
	ref := m.Request.Ref
	if i := strings.LastIndexByte(ref, '.'); i >= 0 {
		ref = ref[i+1:]
	}
	if v, ok := maskFieldByResource[svc+":"+ref]; ok {
		return v
	}
	return "labels"
}

// maskFieldByResource pins a valid update_mask field for a resource whose
// writable fields exclude the generic "labels" default. Keyed by
// "<discovery service>:<request schema name>".
var maskFieldByResource = map[string]string{
	"logging:LogBucket":                "description",
	"logging:LogSink":                  "description",
	"logging:LogExclusion":             "description",
	"logging:LogScope":                 "description",
	"logging:LogView":                  "description",
	"logging:Settings":                 "storageLocation",
	"logging:CmekSettings":             "kmsKeyName",
	"monitoring:Service":               "displayName",
	"monitoring:ServiceLevelObjective": "displayName",
}

// pathOverrides pins the real request path for a method whose Discovery flatPath
// collapses a recursive "{+x=.../**}" resource binding into a single literal
// segment. Firestore's createDocument parent is "…/documents/**", which the
// flatPath renders as one "{documentsId}" segment; filling that yields a
// document-shaped path the (correct) generic codec rejects. The override emits
// the real root-collection shape "…/documents/{collectionId}?documentId=",
// which the harness matches to createDocument through the non-flat "{+parent}"
// template.
var pathOverrides = map[string]string{
	"firestore.projects.databases.documents.createDocument": "v1/projects/{projectsId}/databases/{databasesId}/documents/{collectionId}",
}

// probeTemplate returns the Discovery template a probe should fill: the
// pathOverrides entry when one exists (a recursive binding the flatPath
// collapses), otherwise the flatPath-preferred template.
func probeTemplate(m *Method) string {
	if t, ok := pathOverrides[m.ID]; ok {
		return t
	}
	return templateOf(m)
}

// lastLiteralSegment returns the final literal segment of a template.
func lastLiteralSegment(tpl string) string {
	segs := strings.Split(tpl, "/")
	for i := len(segs) - 1; i >= 0; i-- {
		if _, _, isPH := parsePlaceholder(segs[i]); !isPH {
			return segs[i]
		}
	}
	return ""
}

// requiredQueryValue returns a plausible value for a required query parameter
// that is not an id.
func requiredQueryValue(name string) string {
	switch name {
	case "updateMask", "fieldMask":
		return "labels"
	case "alt":
		return "json"
	case "uploadType":
		return "media"
	}
	return "probe"
}

// urlValues is a small deterministic query builder.
type urlValues map[string]string

func (v urlValues) Set(k, val string) { v[k] = val }

func (v urlValues) Encode() string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+v[k])
	}
	return strings.Join(parts, "&")
}

// synthBody builds a minimal JSON object from a schema including only the
// required fields (recursively). The emulator is deliberately lenient about
// optional/unknown fields, so required-only keeps the probe small.
func synthBody(s *Schema, doc *DiscoveryDoc, depth int) string {
	b, err := json.Marshal(synthValue(s, doc, depth))
	if err != nil {
		return ""
	}
	return string(b)
}

func synthValue(s *Schema, doc *DiscoveryDoc, depth int) any {
	if s == nil || depth > 4 {
		return nil
	}
	if s.Ref != "" {
		if r, ok := doc.ResolveRef(s.Ref); ok {
			s = r
		}
	}
	switch s.Type {
	case "object":
		obj := map[string]any{}
		for _, req := range s.Required {
			if child, ok := s.Properties[req]; ok {
				obj[req] = synthValue(child, doc, depth+1)
			}
		}
		return obj
	case "array":
		if s.Items != nil {
			return []any{synthValue(s.Items, doc, depth+1)}
		}
		return []any{}
	case "integer", "number":
		return 0
	case "boolean":
		return false
	case "string":
		return synthString(s)
	default:
		if len(s.Properties) > 0 {
			return synthValue(&Schema{Type: "object", Properties: s.Properties, Required: s.Required}, doc, depth)
		}
		return ""
	}
}

func synthString(s *Schema) string {
	if len(s.Enum) > 0 {
		return s.Enum[0]
	}
	if s.Format == "byte" {
		return "aGVsbG8="
	}
	return "probe"
}

func sortedServiceKeys(m map[string][]resolvedOp) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
