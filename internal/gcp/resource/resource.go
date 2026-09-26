// Package resource contains GCP resource-name formatting helpers.
//
// GCP identifies resources by hierarchical names rather than ARNs. A resource
// name is a slash-separated path rooted at the owning project, e.g.
// "projects/{project}/topics/{topic}". Some resources are globally named and
// carry no project prefix (GCS buckets).
package resource

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
)

// formatters maps abstract resource types to their GCP resource-name format
// function. To add a new resource type, add one entry here — no switch to update.
var formatters = map[string]func(project, name string) string{
	// Google Cloud Storage — bucket names are global, no project prefix.
	"gcs-bucket": func(_, n string) string { return n },
	"gcs-object": func(_, n string) string { return n },
	// GCS bucket IAM policy resourceId uses a fixed "_" project placeholder.
	"gcs-bucket-policy": func(_, n string) string { return "projects/_/buckets/" + n },
	// GCS notification-config resource name used by the `notificationConfig`
	// event attribute: projects/_/buckets/{bucket}/notificationConfigs/{id}
	// (callers pass "bucket/notificationConfigs/id").
	"gcs-notification": func(_, n string) string { return "projects/_/buckets/" + n },
	// GCS object IAM policy resourceId: projects/_/buckets/{bucket}/objects/{object}.
	"gcs-object-policy": func(_, n string) string { return "projects/_/buckets/" + n },
	// Cloud Pub/Sub
	"pubsub-topic":        func(p, n string) string { return fmt.Sprintf("projects/%s/topics/%s", p, n) },
	"pubsub-subscription": func(p, n string) string { return fmt.Sprintf("projects/%s/subscriptions/%s", p, n) },
	// Secret Manager
	"secret": func(p, n string) string { return fmt.Sprintf("projects/%s/secrets/%s", p, n) },
	// Cloud KMS — names embed the location; callers pass "location/keyRing/cryptoKey".
	"kms-keyring": func(p, n string) string {
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", p, locOf(n), ringOf(n))
	},
	"kms-cryptokey": func(p, n string) string {
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", p, locOf(n), ringOf(n), keyOf(n))
	},
	"kms-cryptokey-version": func(p, n string) string {
		// callers pass "location/keyRing/cryptoKey/version"
		loc, kr, k, v := parts4(n)
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", p, loc, kr, k, v)
	},
	// IAM — service accounts are identified by their email in the full name.
	"service-account": func(p, n string) string { return fmt.Sprintf("projects/%s/serviceAccounts/%s", p, n) },
	// Firestore — document names: projects/{p}/databases/{db}/documents/{path}.
	// The name argument is the relative path after the project, i.e.
	// "databases/{db}/documents/{path}".
	"firestore-document": func(p, n string) string { return fmt.Sprintf("projects/%s/%s", p, n) },
	// Cloud Functions — names embed the location; callers pass "location/function",
	// "location" (for location discovery), and "location/operation".
	"cloud-function": func(p, n string) string {
		loc, fn := fnLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/functions/%s", p, loc, fn)
	},
	"cloud-function-location": func(p, n string) string {
		return fmt.Sprintf("projects/%s/locations/%s", p, n)
	},
	"cloud-function-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// The backing Cloud Run service of a Cloud Functions v2 function is
	// "projects/{p}/locations/{l}/services/{id}"; callers pass "location/id".
	// It is reported as Cloud Functions' serviceConfig.service (output-only),
	// not as a Cloud Run emulation surface. The path shape matches
	// metastore-service, but this is a distinct resource type so the two do not
	// drift when either service evolves.
	"cloud-run-service": func(p, n string) string {
		loc, s := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/services/%s", p, loc, s)
	},
	// Cloud Workflows — names embed the location; callers pass "location/workflow".
	"workflow": func(p, n string) string {
		loc, wf := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/workflows/%s", p, loc, wf)
	},
	// Workflow executions — callers pass "location/workflow/execution".
	"workflow-execution": func(p, n string) string {
		loc, wf, ex := wfExec(n)
		return fmt.Sprintf("projects/%s/locations/%s/workflows/%s/executions/%s", p, loc, wf, ex)
	},
	// Workflow long-running operations — callers pass "location/operation".
	"workflow-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// Cloud Dataproc — names embed the region; callers pass "region/name".
	// Dataproc uses "regions" (not "locations") and its operations are the
	// google.longrunning operations under /regions/{region}/operations/{id}.
	"dataproc-cluster": func(p, n string) string {
		reg, c := regionOf(n)
		return fmt.Sprintf("projects/%s/regions/%s/clusters/%s", p, reg, c)
	},
	"dataproc-job": func(p, n string) string {
		reg, j := regionOf(n)
		return fmt.Sprintf("projects/%s/regions/%s/jobs/%s", p, reg, j)
	},
	"dataproc-operation": func(p, n string) string {
		reg, op := regionOf(n)
		return fmt.Sprintf("projects/%s/regions/%s/operations/%s", p, reg, op)
	},
	// Managed Kafka (Apache Kafka for BigQuery) — names embed the location;
	// callers pass "location/cluster" and "location/cluster/topic".
	"managedkafka-cluster": func(p, n string) string {
		loc, c := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", p, loc, c)
	},
	"managedkafka-topic": func(p, n string) string {
		loc, c, t := wfExec(n)
		return fmt.Sprintf("projects/%s/locations/%s/clusters/%s/topics/%s", p, loc, c, t)
	},
	// Managed Kafka long-running operations — callers pass "location/operation".
	// The path is shared with Workflows' LRO surface on a single host, so the
	// emulator returns operations inline (done:true); the formatter exists for
	// direct operations.get/list dispatch.
	"managedkafka-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// Managed Kafka ACLs — callers pass "location/cluster/aclId". An acl id may
	// itself contain a "/" (e.g. "topic/my-topic"), so the leaf is everything
	// after the second segment.
	"managedkafka-acl": func(p, n string) string {
		loc, cluster, acl := mkTriple(n)
		return fmt.Sprintf("projects/%s/locations/%s/clusters/%s/acls/%s", p, loc, cluster, acl)
	},
	// Managed Kafka consumer groups — callers pass "location/cluster/group".
	"managedkafka-consumergroup": func(p, n string) string {
		loc, cluster, group := mkTriple(n)
		return fmt.Sprintf("projects/%s/locations/%s/clusters/%s/consumerGroups/%s", p, loc, cluster, group)
	},
	// Dataproc Metastore (control plane) — names embed the location; callers pass
	// "location/service", "location/service/backup",
	// "location/service/import", and "location/operation".
	"metastore-service": func(p, n string) string {
		loc, s := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/services/%s", p, loc, s)
	},
	"metastore-backup": func(p, n string) string {
		loc, s, b := wfExec(n)
		return fmt.Sprintf("projects/%s/locations/%s/services/%s/backups/%s", p, loc, s, b)
	},
	"metastore-metadata-import": func(p, n string) string {
		loc, s, m := wfExec(n)
		return fmt.Sprintf("projects/%s/locations/%s/services/%s/metadataImports/%s", p, loc, s, m)
	},
	"metastore-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// Eventarc — names embed the location; callers pass "location/trigger",
	// "location/channel", "location/provider", and "location/operation".
	"eventarc-trigger": func(p, n string) string {
		loc, t := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/triggers/%s", p, loc, t)
	},
	"eventarc-channel": func(p, n string) string {
		loc, c := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/channels/%s", p, loc, c)
	},
	"eventarc-provider": func(p, n string) string {
		loc, pr := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/providers/%s", p, loc, pr)
	},
	"eventarc-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// Memorystore for Redis — names embed the location; callers pass
	// "location/instance" and "location/operation".
	"memorystore-instance": func(p, n string) string {
		loc, id := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/instances/%s", p, loc, id)
	},
	"memorystore-location": func(p, n string) string {
		return fmt.Sprintf("projects/%s/locations/%s", p, n)
	},
	"memorystore-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// Cloud DNS — the v1 REST wire uses bare names (managed zone "name",
	// rrset "name"), so these formatters are not exercised by the REST
	// provider; they exist for the shared resource-name surface. Callers pass
	// "zone", "zone/name/type", and "zone/changeId".
	"clouddns-managed-zone": func(p, n string) string {
		return fmt.Sprintf("projects/%s/managedZones/%s", p, n)
	},
	"clouddns-rrset": func(p, n string) string {
		z, nm, t := wfExec(n)
		return fmt.Sprintf("projects/%s/managedZones/%s/rrsets/%s/%s", p, z, nm, t)
	},
	"clouddns-change": func(p, n string) string {
		z, id := wfLoc(n)
		return fmt.Sprintf("projects/%s/managedZones/%s/changes/%s", p, z, id)
	},
	// Cloud SQL Admin — relative resource names used to build selfLink and
	// targetLink. Callers pass "instance", "instance/database",
	// "instance/user", and "operation".
	"cloudsql-instance": func(p, n string) string {
		return fmt.Sprintf("projects/%s/instances/%s", p, n)
	},
	"cloudsql-database": func(p, n string) string {
		inst, db := wfLoc(n)
		return fmt.Sprintf("projects/%s/instances/%s/databases/%s", p, inst, db)
	},
	"cloudsql-user": func(p, n string) string {
		inst, usr := wfLoc(n)
		return fmt.Sprintf("projects/%s/instances/%s/users/%s", p, inst, usr)
	},
	"cloudsql-operation": func(p, n string) string {
		return fmt.Sprintf("projects/%s/operations/%s", p, n)
	},
	// BigQuery — datasets/tables/jobs carry a projectId but no "name" field on
	// the REST wire (they use datasetReference/tableReference/jobReference and
	// the opaque id), so these formatters are not exercised by the v2 REST
	// provider; they exist for the resource-name surface shared with any future
	// gRPC/management-plane representation.
	"bigquery-dataset": func(p, n string) string {
		return fmt.Sprintf("projects/%s/datasets/%s", p, n)
	},
	"bigquery-table": func(p, n string) string {
		d, t := bqTableOf(n)
		return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", p, d, t)
	},
	"bigquery-job": func(p, n string) string {
		return fmt.Sprintf("projects/%s/jobs/%s", p, n)
	},
	// Compute Engine — relative resource names used to build selfLink and
	// targetLink. Zonal resources pass "zone/name"; regional resources pass
	// "region/name"; global resources pass "name"; operations pass
	// "zone/op" or "region/op" (and the global formatter passes "op").
	"compute-instance": func(p, n string) string {
		z, name := wfLoc(n)
		return fmt.Sprintf("projects/%s/zones/%s/instances/%s", p, z, name)
	},
	"compute-disk": func(p, n string) string {
		z, name := wfLoc(n)
		return fmt.Sprintf("projects/%s/zones/%s/disks/%s", p, z, name)
	},
	"compute-disk-type": func(p, n string) string {
		z, name := wfLoc(n)
		return fmt.Sprintf("projects/%s/zones/%s/diskTypes/%s", p, z, name)
	},
	"compute-machine-type": func(p, n string) string {
		z, name := wfLoc(n)
		return fmt.Sprintf("projects/%s/zones/%s/machineTypes/%s", p, z, name)
	},
	"compute-network": func(p, n string) string {
		return fmt.Sprintf("projects/%s/global/networks/%s", p, n)
	},
	"compute-firewall": func(p, n string) string {
		return fmt.Sprintf("projects/%s/global/firewalls/%s", p, n)
	},
	"compute-subnetwork": func(p, n string) string {
		r, name := wfLoc(n)
		return fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", p, r, name)
	},
	"compute-zone": func(p, n string) string {
		return fmt.Sprintf("projects/%s/zones/%s", p, n)
	},
	"compute-region": func(p, n string) string {
		return fmt.Sprintf("projects/%s/regions/%s", p, n)
	},
	"compute-zone-operation": func(p, n string) string {
		z, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/zones/%s/operations/%s", p, z, op)
	},
	"compute-region-operation": func(p, n string) string {
		r, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/regions/%s/operations/%s", p, r, op)
	},
	"compute-global-operation": func(p, n string) string {
		return fmt.Sprintf("projects/%s/global/operations/%s", p, n)
	},
	// Service Usage v1 — a service is "projects/{project}/services/{service}";
	// the parent is "projects/{project}", and mutations return a globally-named
	// google.longrunning Operation "operations/{id}".
	"serviceusage-service": func(p, n string) string {
		return fmt.Sprintf("projects/%s/services/%s", p, n)
	},
	"serviceusage-parent": func(p, _ string) string {
		return fmt.Sprintf("projects/%s", p)
	},
	"serviceusage-operation": func(_, n string) string {
		return "operations/" + n
	},
	// Cloud Resource Manager — a project's canonical v3 resource name is
	// "projects/{project}". The v1 REST API has no resource name (it identifies
	// a project by projectId), so callers pass the project id as the closure
	// project and the name is unused.
	"project": func(p, _ string) string {
		return "projects/" + p
	},
	// Cloud Logging v2 (gRPC and REST).
	"log": func(p, n string) string { return fmt.Sprintf("projects/%s/logs/%s", p, n) },
	// Cloud Monitoring v3 (gRPC and REST).
	"metric-descriptor": func(p, n string) string {
		return fmt.Sprintf("projects/%s/metricDescriptors/%s", p, n)
	},
	"alert-policy": func(p, n string) string { return fmt.Sprintf("projects/%s/alertPolicies/%s", p, n) },
	"notification-channel": func(p, n string) string {
		return fmt.Sprintf("projects/%s/notificationChannels/%s", p, n)
	},
	"notification-channel-descriptor": func(p, n string) string {
		return fmt.Sprintf("projects/%s/notificationChannelDescriptors/%s", p, n)
	},
	"monitored-resource-descriptor": func(p, n string) string {
		return fmt.Sprintf("projects/%s/monitoredResourceDescriptors/%s", p, n)
	},
}

// ProjectNumber returns a synthesized, stable 12-digit decimal project number
// derived from the project ID via FNV-1a. The emulator has no real GCP project
// number, so the value is informational only — used for fields such as
// storage#bucket.projectNumber where the wire shape requires a number.
func ProjectNumber(project string) string {
	h := fnv.New64a()
	h.Write([]byte(project))
	return fmt.Sprintf("%012d", h.Sum64()%1_000_000_000_000)
}

// ResourceID returns a function that formats GCP resource names for a project.
// Inject the result into NormalizedRequest.ResourceID at the gateway layer.
func ResourceID(project string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		if f, ok := formatters[resourceType]; ok {
			return f(project, name)
		}
		slog.Warn("gcp/resource.ResourceID: unknown resource type, returning name as-is",
			"resourceType", resourceType, "name", name)
		return name
	}
}

// locOf splits a "location/keyRing[/cryptoKey]" name into its location part.
func locOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			return name[:i]
		}
	}
	return "global"
}

// ringOf returns the keyRing segment of a "location/keyRing[/cryptoKey]" name.
func ringOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			rest := name[i+1:]
			for j := 0; j < len(rest); j++ {
				if rest[j] == '/' {
					return rest[:j]
				}
			}
			return rest
		}
	}
	return name
}

// keyOf returns the cryptoKey segment of a "location/keyRing/cryptoKey" name.
func keyOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			rest := name[i+1:]
			for j := 0; j < len(rest); j++ {
				if rest[j] == '/' {
					return rest[j+1:]
				}
			}
			return ""
		}
	}
	return ""
}

// parts4 splits a "location/keyRing/cryptoKey/version" name.
func parts4(name string) (loc, ring, key, ver string) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 4:
		return parts[0], parts[1], parts[2], parts[3]
	case 3:
		return parts[0], parts[1], parts[2], ""
	default:
		return "global", name, "", ""
	}
}

// fnLoc splits a "location/function" name. A name with no slash defaults the
// location to "-" (the GCP wildcard region).
func fnLoc(name string) (loc, fn string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "-", name
}

// wfLoc splits a "location/workflow" name. A name with no slash defaults the
// location to "-".
func wfLoc(name string) (loc, wf string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "-", name
}

// regionOf splits a "region/name" Dataproc name. A name with no slash
// defaults the region to "global".
func regionOf(name string) (region, id string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "global", name
}

// wfExec splits a "location/workflow/execution" name.
func wfExec(name string) (loc, wf, ex string) {
	parts := strings.Split(name, "/")
	if len(parts) >= 3 {
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1], ""
	}
	return "-", name, ""
}

// mkTriple splits "location/cluster/leaf" into its three parts, keeping any
// remaining slashes in the leaf (an acl id or consumer-group id may contain a
// "/").
func mkTriple(name string) (loc, cluster, leaf string) {
	parts := strings.SplitN(name, "/", 3)
	switch len(parts) {
	case 3:
		return parts[0], parts[1], parts[2]
	case 2:
		return parts[0], parts[1], ""
	default:
		return "-", name, ""
	}
}

// bqTableOf splits a "datasetId/tableId" name into its two segments. A name
// with no slash defaults the dataset to "-".
func bqTableOf(name string) (dataset, table string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "-", name
}
