//go:build gcp_conformance

package gcpconformance

import (
	"sort"
	"strings"
)

// actionOverrides maps an emulator registry dispatch key (ProviderPrefix.Action)
// to a Discovery method id when the CamelCase heuristic cannot derive it. Most
// entries are nested resource paths the flat registry action name elides.
var actionOverrides = map[string]string{
	// GCS legacy ACL sub-resources are named "…AccessControls" in Discovery.
	"Storage.BucketACLList":   "storage.bucketAccessControls.list",
	"Storage.BucketACLInsert": "storage.bucketAccessControls.insert",
	"Storage.ObjectACLList":   "storage.objectAccessControls.list",
	"Storage.ObjectACLInsert": "storage.objectAccessControls.insert",
	// Media download / resumable-upload are the same objects.* methods on the
	// wire (alt=media and the resumable upload session).
	"Storage.ObjectsGetMedia":             "storage.objects.get",
	"Storage.ObjectsInsertStartResumable": "storage.objects.insert",
	"Storage.ObjectsInsertResumable":      "storage.objects.insert",

	// Secret Manager version verbs are 1:1 with Discovery's versions.* methods.
	"Secret.GetVersion":     "secretmanager.projects.secrets.versions.get",
	"Secret.DestroyVersion": "secretmanager.projects.secrets.versions.destroy",
	"Secret.DisableVersion": "secretmanager.projects.secrets.versions.disable",
	"Secret.EnableVersion":  "secretmanager.projects.secrets.versions.enable",

	// IAM service-account keys nest under serviceAccounts in Discovery.
	"IAM.ServiceAccountKeyCreate": "iam.projects.serviceAccounts.keys.create",
	"IAM.ServiceAccountKeyGet":    "iam.projects.serviceAccounts.keys.get",
	"IAM.ServiceAccountKeyList":   "iam.projects.serviceAccounts.keys.list",
	"IAM.ServiceAccountKeyDelete": "iam.projects.serviceAccounts.keys.delete",
	// keys.disable / keys.enable (J45) also nest under serviceAccounts.
	"IAM.ServiceAccountKeyDisable": "iam.projects.serviceAccounts.keys.disable",
	"IAM.ServiceAccountKeyEnable":  "iam.projects.serviceAccounts.keys.enable",
	// GetAllowedLocations is served for both serviceAccounts and
	// workloadIdentityPools (J65); pin the cell to the serviceAccounts method
	// (the heuristic otherwise picks workforcePools alphabetically).
	"IAMCredentials.GetAllowedLocations": "iamcredentials.projects.serviceAccounts.getAllowedLocations",
	// serviceAccounts.patch (PATCH, updateMask) and serviceAccounts.update
	// (PUT, full replace) share one handler.
	"IAM.ServiceAccountPatch":  "iam.projects.serviceAccounts.patch",
	"IAM.ServiceAccountUpdate": "iam.projects.serviceAccounts.update",

	// KMS crypto-key-version public key is Discovery's cryptoKeyVersions.getPublicKey.
	"KMS.CryptoKeyVersionGetPublicKey": "cloudkms.projects.locations.keyRings.cryptoKeys.cryptoKeyVersions.getPublicKey",

	// Remaining resource/verb naming differences across services.
	"CloudSQL.ConnectGet":                        "sql.connect.get",
	"Compute.OperationsGet":                      "compute.globalOperations.get",
	"Compute.OperationsList":                     "compute.globalOperations.list",
	"Dataproc.SubmitJobAsOperation":              "dataproc.projects.regions.jobs.submitAsOperation",
	"Dataproc.CreateWorkflowTemplate":            "dataproc.projects.regions.workflowTemplates.create",
	"Dataproc.GetWorkflowTemplate":               "dataproc.projects.regions.workflowTemplates.get",
	"Dataproc.ListWorkflowTemplates":             "dataproc.projects.regions.workflowTemplates.list",
	"Dataproc.UpdateWorkflowTemplate":            "dataproc.projects.regions.workflowTemplates.update",
	"Dataproc.DeleteWorkflowTemplate":            "dataproc.projects.regions.workflowTemplates.delete",
	"Dataproc.InstantiateWorkflowTemplate":       "dataproc.projects.regions.workflowTemplates.instantiate",
	"Dataproc.InstantiateInlineWorkflowTemplate": "dataproc.projects.regions.workflowTemplates.instantiateInline",
	"Metastore.AlterMetadataResourceLocation":    "metastore.projects.locations.services.alterLocation",

	// BigQuery's tabledata/jobs/projects surfaces use different method names
	// than the emulator's registry actions.
	"BigQuery.InsertAll":         "bigquery.tabledata.insertAll",
	"BigQuery.ListRows":          "bigquery.tabledata.list",
	"BigQuery.Query":             "bigquery.jobs.query",
	"BigQuery.GetQueryResults":   "bigquery.jobs.getQueryResults",
	"BigQuery.GetServiceAccount": "bigquery.projects.getServiceAccount",

	// Service Usage v1 services.* verbs.
	"ServiceUsage.ServicesList":        "serviceusage.services.list",
	"ServiceUsage.ServicesGet":         "serviceusage.services.get",
	"ServiceUsage.ServicesBatchEnable": "serviceusage.services.batchEnable",
	"ServiceUsage.ServicesEnable":      "serviceusage.services.enable",
	"ServiceUsage.ServicesDisable":     "serviceusage.services.disable",

	// Monitoring's custom-verb and verification-code actions do not derive
	// from the CamelCase heuristic.
	"Monitoring.CreateServiceTimeSeries":                 "monitoring.projects.timeSeries.createService",
	"Monitoring.SendNotificationChannelVerificationCode": "monitoring.projects.notificationChannels.sendVerificationCode",
	"Monitoring.GetNotificationChannelVerificationCode":  "monitoring.projects.notificationChannels.getVerificationCode",

	// Managed Kafka ACL custom-verb methods do not derive from the CamelCase
	// heuristic (the resource segment is nested under clusters.acls).
	"ManagedKafka.AddAclEntry":    "managedkafka.projects.locations.clusters.acls.addAclEntry",
	"ManagedKafka.RemoveAclEntry": "managedkafka.projects.locations.clusters.acls.removeAclEntry",

	// Cloud Run v2 (run.googleapis.com) nested method ids that the CamelCase
	// heuristic cannot derive from the flat registry action names.
	"Run.CreateService":      "run.projects.locations.services.create",
	"Run.GetService":         "run.projects.locations.services.get",
	"Run.ListServices":       "run.projects.locations.services.list",
	"Run.UpdateService":      "run.projects.locations.services.patch",
	"Run.DeleteService":      "run.projects.locations.services.delete",
	"Run.GetIamPolicy":       "run.projects.locations.services.getIamPolicy",
	"Run.SetIamPolicy":       "run.projects.locations.services.setIamPolicy",
	"Run.TestIamPermissions": "run.projects.locations.services.testIamPermissions",
	"Run.ListRevisions":      "run.projects.locations.services.revisions.list",
	"Run.GetRevision":        "run.projects.locations.services.revisions.get",
	// The REST :wait custom method maps to operations.get. CancelOperation is
	// served for direct dispatch but real run v2 has no operations.cancel
	// discovery method, so it stays uncovered.
	"Run.WaitOperation":   "run.projects.locations.operations.get",
	"Run.GetOperation":    "run.projects.locations.operations.get",
	"Run.ListOperations":  "run.projects.locations.operations.list",
	"Run.DeleteOperation": "run.projects.locations.operations.delete",

	// Cloud Resource Manager v1 project surface (the discovery document's
	// service is "cloudresourcemanager"; the vendored snapshot is keyed by the
	// emulator's wire service name "resourcemanager").
	"ResourceManager.ProjectGet":                "cloudresourcemanager.projects.get",
	"ResourceManager.ProjectGetIamPolicy":       "cloudresourcemanager.projects.getIamPolicy",
	"ResourceManager.ProjectSetIamPolicy":       "cloudresourcemanager.projects.setIamPolicy",
	"ResourceManager.ProjectTestIamPermissions": "cloudresourcemanager.projects.testIamPermissions",
}

// ActionResolver maps emulator registry actions to Discovery method ids within
// a service's snapshot.
type ActionResolver struct {
	methodIDs map[string][]string // snapshot basename -> full method ids
}

// NewActionResolver builds a resolver over all loaded documents.
func NewActionResolver(docs map[string]*DiscoveryDoc) *ActionResolver {
	r := &ActionResolver{methodIDs: map[string][]string{}}
	for svc, doc := range docs {
		doc.WalkMethods(func(m *Method) {
			if m.ID != "" {
				r.methodIDs[svc] = append(r.methodIDs[svc], m.ID)
			}
		})
	}
	for svc := range r.methodIDs {
		sort.Strings(r.methodIDs[svc])
	}
	return r
}

// Resolve returns the Discovery method id for an operation, if one can be
// derived. The override table wins; otherwise every CamelCase split of the
// action is tried as "resource.method", and finally a normalised suffix match
// handles naming differences (singular/plural, verb-first actions).
func (r *ActionResolver) Resolve(op Operation) (string, bool) {
	ids, ok := r.methodIDs[op.Service]
	if !ok || len(ids) == 0 {
		return "", false
	}
	if override, ok := actionOverrides[op.Key()]; ok {
		if containsString(ids, override) {
			return override, true
		}
		return override, false
	}
	for _, cand := range actionCandidates(op.Action) {
		for _, id := range ids {
			tail := methodTail(id)
			if tail == cand || strings.HasSuffix(tail, "."+cand) {
				return id, true
			}
		}
	}
	if id, ok := normalizedMatch(op.Action, ids); ok {
		return id, true
	}
	return "", false
}

// methodTail strips the leading service-name segment from a Discovery method id
// (e.g. "storage.buckets.list" -> "buckets.list").
func methodTail(id string) string {
	if i := strings.IndexByte(id, '.'); i >= 0 {
		return id[i+1:]
	}
	return id
}

// actionCandidates derives Discovery-style "resource.method" candidates from a
// registry action by trying every CamelCase split point, in both resource-first
// ("ObjectsInsert" -> objects.insert) and verb-first ("CancelJob" ->
// jobs.cancel) orders, plus verb synonyms (create/insert, update/patch).
func actionCandidates(action string) []string {
	words := splitCamel(action)
	if len(words) == 0 {
		return nil
	}
	var cands []string
	seen := map[string]bool{}
	add := func(c string) {
		if c == "" || seen[c] {
			return
		}
		seen[c] = true
		cands = append(cands, c)
		for _, syn := range verbSynonyms(c) {
			if !seen[syn] {
				seen[syn] = true
				cands = append(cands, syn)
			}
		}
	}
	for k := 1; k < len(words); k++ {
		// Resource-first: the emulator's dominant naming (Storage/PubSub/KMS/…).
		add(pluralizeCamel(lowerCamel(words[:k])) + "." + lowerCamel(words[k:]))
		// Verb-first: BigQuery/CloudSQL-style actions (CreateDataset, …).
		add(pluralizeCamel(lowerCamel(words[k:])) + "." + lowerCamel(words[:k]))
	}
	add(lowerCamel(words))
	return cands
}

// verbSynonyms maps Discovery/registry verb spellings onto each other.
func verbSynonyms(cand string) []string {
	if i := strings.LastIndexByte(cand, '.'); i >= 0 {
		head, verb := cand[:i+1], cand[i+1:]
		switch verb {
		case "create":
			return []string{head + "insert"}
		case "insert":
			return []string{head + "create"}
		case "update":
			return []string{head + "patch"}
		case "patch":
			return []string{head + "update"}
		}
		return nil
	}
	switch cand {
	case "create":
		return []string{"insert"}
	case "insert":
		return []string{"create"}
	case "update":
		return []string{"patch"}
	case "patch":
		return []string{"update"}
	}
	return nil
}

// normalizedMatch is a best-effort fallback: it singularises words in the
// action and every method tail and accepts a suffix relationship.
func normalizedMatch(action string, ids []string) (string, bool) {
	a := normalizeWords(action)
	if a == "" {
		return "", false
	}
	for _, id := range ids {
		m := normalizeWords(methodTail(id))
		if m == a || strings.HasSuffix(m, a) || strings.HasSuffix(a, m) {
			return id, true
		}
	}
	return "", false
}

func normalizeWords(s string) string {
	var b strings.Builder
	for _, w := range splitCamel(s) {
		b.WriteString(singularizeWord(strings.ToLower(w)))
	}
	return b.String()
}

// splitCamel splits an identifier into CamelCase words. Consecutive capitals
// stay together ("ACLList" -> ["ACL","List"]).
func splitCamel(s string) []string {
	runes := []rune(s)
	if len(runes) == 0 {
		return nil
	}
	var words []string
	start := 0
	for i := 1; i < len(runes); i++ {
		prev, cur := runes[i-1], runes[i]
		var next rune
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		if isUpper(cur) {
			if isLower(prev) || isDigit(prev) || (isUpper(prev) && next != 0 && isLower(next)) {
				words = append(words, string(runes[start:i]))
				start = i
			}
		}
	}
	words = append(words, string(runes[start:]))
	return words
}

func lowerCamel(words []string) string {
	if len(words) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(strings.ToLower(words[0]))
	for _, w := range words[1:] {
		b.WriteString(w)
	}
	return b.String()
}

func pluralizeCamel(s string) string {
	words := splitCamel(s)
	if len(words) == 0 {
		return s
	}
	words[len(words)-1] = pluralizeWord(words[len(words)-1])
	return strings.Join(words, "")
}

func pluralizeWord(w string) string {
	if w == "" {
		return w
	}
	lower := strings.ToLower(w)
	// Registry resource words are usually already plural ("Objects", "Disks");
	// assume a trailing "s" means plural rather than appending another "es".
	if strings.HasSuffix(lower, "s") {
		return w
	}
	if strings.HasSuffix(lower, "x") || strings.HasSuffix(lower, "ch") ||
		strings.HasSuffix(lower, "sh") || strings.HasSuffix(lower, "z") {
		return w + "es"
	}
	if strings.HasSuffix(lower, "y") && len(w) > 1 && !isVowel(rune(lower[len(lower)-2])) {
		return w[:len(w)-1] + "ies"
	}
	return w + "s"
}

func singularizeWord(w string) string {
	switch {
	case strings.HasSuffix(w, "ies") && len(w) > 3:
		return w[:len(w)-3] + "y"
	case strings.HasSuffix(w, "sses"), strings.HasSuffix(w, "shes"),
		strings.HasSuffix(w, "ches"), strings.HasSuffix(w, "xes"), strings.HasSuffix(w, "zes"):
		return w[:len(w)-2]
	case strings.HasSuffix(w, "s") && !strings.HasSuffix(w, "ss"):
		return w[:len(w)-1]
	}
	return w
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }
func isLower(r rune) bool { return r >= 'a' && r <= 'z' }
func isDigit(r rune) bool { return r >= '0' && r <= '9' }
func isVowel(r rune) bool {
	switch r {
	case 'a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U':
		return true
	}
	return false
}
