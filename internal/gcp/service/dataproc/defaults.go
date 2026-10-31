package dataproc

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// Compute-Engine cluster read defaults.
//
// A real Dataproc cluster always reports the fields below. Discovery documents
// SoftwareConfig.imageVersion as defaulting to "the latest Debian version",
// GceClusterConfig.zoneUri as "On a get request, zone will always be present",
// DiskConfig.bootDiskSizeGb as 500 GB, InstanceGroupConfig.numInstances as 1 for
// a standard cluster's master, and the create-cluster guide describes the
// default shape as "the default configuration (1 master, 2 workers)" on
// n1-standard-4. The emulator materialises them at render time — never in the
// stored config, so a caller's request stays byte-identical — because clients
// read the response unguarded: the hashicorp/google provider's
// flattenSoftwareConfig dereferences config.softwareConfig.imageVersion with no
// nil check, so an absent message panics the provider on the read-back of a
// google_dataproc_cluster with a plain cluster_config.
//
// Caller-supplied Compute Engine links (`gceClusterConfig.zoneUri`,
// `networkUri`, `subnetworkUri` and the instance groups' `machineTypeUri`) are
// canonicalised to the same absolute self-link form at render time: a real get
// answers a link even when the create named the resource by short name, and the
// stored config keeps whatever the caller sent.
const (
	// defaultGCEImageVersion is the image the emulator reports when the caller
	// does not pin softwareConfig.imageVersion. Dataproc moves the "latest
	// Debian" alias forward as a new minor goes GA; the 2.3 track is the current
	// GA Debian default and 2.3.3-debian12 is the latest sub-minor its release
	// notes document. A concrete sub-minor is what a real get returns, and the
	// provider's DataprocImageVersionDiffSuppress treats a family-form request
	// ("2.3") as equal to a concrete response. Bumping it also means re-recording
	// the REST conformance transcript and the SDK/differential expectations.
	defaultGCEImageVersion = "2.3.3-debian12"
	defaultMachineType     = "n1-standard-4"
	defaultBootDiskSizeGB  = 500
	// A standard (non-HA) cluster has exactly one master and, by default, two
	// primary workers.
	defaultMasterInstances = 1
	defaultWorkerInstances = 2
)

// withGCEDefaults returns config with every documented read-time default filled
// in and the caller's Compute Engine links canonicalised. It never mutates the
// stored bytes: it runs on the freshly unmarshalled render copy.
func withGCEDefaults(c dpstore.Cluster, config map[string]any) map[string]any {
	if config == nil {
		config = map[string]any{}
	}
	gcc := childMap(config, "gceClusterConfig")
	// The cluster's zone drives the instance groups' machineTypeUri, so resolve
	// it from the caller's own zoneUri when they named one.
	zone := zoneFromURI(bodyString(gcc, "zoneUri"))
	if zone == "" {
		zone = defaultZone(c.Region)
	}
	if gcc != nil {
		canonicaliseGCELinks(gcc, c, zone)
	}
	if sw := childMap(config, "softwareConfig"); sw != nil {
		if strings.TrimSpace(bodyString(sw, "imageVersion")) == "" {
			sw["imageVersion"] = defaultGCEImageVersion
		}
	}
	if master := childMap(config, "masterConfig"); master != nil {
		fillInstanceGroup(master, defaultMasterInstances, c.ProjectID, zone)
		canonicaliseMachineTypeURI(master, c.ProjectID, zone)
	}
	if worker := childMap(config, "workerConfig"); worker != nil {
		fillInstanceGroup(worker, defaultWorkerInstances, c.ProjectID, zone)
		canonicaliseMachineTypeURI(worker, c.ProjectID, zone)
	}
	// Secondary workers have no documented default count (0 means "no secondary
	// workers"), so only their caller-supplied machine type is canonicalised —
	// the group itself is never materialised.
	if secondary := objectField(config, "secondaryWorkerConfig"); secondary != nil {
		canonicaliseMachineTypeURI(secondary, c.ProjectID, zone)
	}
	return config
}

// canonicaliseGCELinks rewrites a GceClusterConfig's caller-supplied
// zoneUri/networkUri/subnetworkUri to the absolute Compute Engine self-link form
// real Dataproc returns on every read. Clients send a short name ("us-central1-a",
// "default"), a partial resource name ("projects/p/global/networks/default") or a
// full URL; a real get only ever answers the full URL. A value whose shape cannot
// be resolved is left exactly as it was sent rather than replaced by a guess.
func canonicaliseGCELinks(gcc map[string]any, c dpstore.Cluster, zone string) {
	if strings.TrimSpace(bodyString(gcc, "zoneUri")) == "" {
		// A real get always names a zone, so an unset one is filled with the
		// region default; an unparseable caller value is left as sent below.
		gcc["zoneUri"] = resource.ComputeLink(c.ProjectID, "compute-zone", zone)
	} else if z := zoneFromURI(bodyString(gcc, "zoneUri")); z != "" {
		gcc["zoneUri"] = resource.ComputeLinkFor(bodyString(gcc, "zoneUri"), c.ProjectID, "compute-zone", z)
	}
	if raw := bodyString(gcc, "networkUri"); strings.TrimSpace(raw) != "" {
		if name := resource.LinkTail(raw); name != "" {
			gcc["networkUri"] = resource.ComputeLinkFor(raw, c.ProjectID, "compute-network", name)
		}
	}
	if raw := bodyString(gcc, "subnetworkUri"); strings.TrimSpace(raw) != "" {
		if name := resource.LinkTail(raw); name != "" {
			region := resource.PathSegmentAfter(raw, "regions")
			if region == "" {
				region = c.Region
			}
			gcc["subnetworkUri"] = resource.ComputeLinkFor(raw, c.ProjectID, "compute-subnetwork", region+"/"+name)
		}
	}
}

// canonicaliseMachineTypeURI rewrites a caller-supplied instance group machine
// type to the full self-link, taking the machine type's own name (and the zone,
// when the caller named one) from what was sent.
func canonicaliseMachineTypeURI(group map[string]any, project, zone string) {
	raw := bodyString(group, "machineTypeUri")
	if strings.TrimSpace(raw) == "" {
		return
	}
	name := resource.LinkTail(raw)
	if name == "" {
		return
	}
	if z := resource.PathSegmentAfter(raw, "zones"); z != "" {
		zone = z
	}
	group["machineTypeUri"] = resource.ComputeLinkFor(raw, project, "compute-machine-type", zone+"/"+name)
}

// fillInstanceGroup fills an instance group's documented defaults (count,
// machine type, boot disk) where the caller left them unset. Zero-valued
// defaults (numLocalSsds) are not rendered: protojson omits zero scalars, so
// writing one would make the REST body carry a field the gRPC body drops.
func fillInstanceGroup(group map[string]any, numInstances int, project, zone string) {
	if _, ok := group["numInstances"]; !ok {
		group["numInstances"] = numInstances
	}
	if strings.TrimSpace(bodyString(group, "machineTypeUri")) == "" {
		group["machineTypeUri"] = resource.ComputeLink(project, "compute-machine-type", zone+"/"+defaultMachineType)
	}
	disk, isMap := group["diskConfig"].(map[string]any)
	if !isMap {
		if v, present := group["diskConfig"]; present && v != nil {
			return // a non-object diskConfig is left alone, never overwritten
		}
		disk = map[string]any{}
		group["diskConfig"] = disk
	}
	if _, ok := disk["bootDiskSizeGb"]; !ok {
		disk["bootDiskSizeGb"] = defaultBootDiskSizeGB
	}
}

// childMap returns parent[key] as an object, creating an empty one when the key
// is absent or explicitly null (protojson reads a null message field as unset).
// It returns nil when the caller sent a non-object value, so a malformed field
// is left alone instead of being clobbered by defaults.
func childMap(parent map[string]any, key string) map[string]any {
	if v, ok := parent[key]; ok && v != nil {
		m, _ := v.(map[string]any)
		return m
	}
	m := map[string]any{}
	parent[key] = m
	return m
}

// objectField returns parent[key] when it is an object, without creating it: a
// read-side fill must never materialise a group the caller did not send.
func objectField(parent map[string]any, key string) map[string]any {
	m, _ := parent[key].(map[string]any)
	return m
}

// zoneFromURI extracts the zone from a Compute Engine zoneUri, which real
// clients send as a full URL, a partial URI (projects/{p}/zones/{z}) or a short
// name. It returns "" when the value does not end in something shaped like a
// zone, so a malformed URI falls back to the region default instead of leaking a
// bogus segment into machineTypeUri.
func zoneFromURI(uri string) string {
	zone := resource.LinkTail(uri)
	if !plausibleZone(zone) {
		return ""
	}
	return zone
}

// plausibleZone reports whether s has a GCP zone's shape (us-central1-a): three
// dash-separated parts, the last a single zone letter.
func plausibleZone(s string) bool {
	parts := strings.Split(s, "-")
	return len(parts) == 3 && parts[0] != "" && parts[1] != "" && len(parts[2]) == 1
}

// defaultZone picks the zone the service reports for a cluster that names none:
// real Dataproc chooses one in the cluster's region, so the emulator reports the
// region's first zone.
func defaultZone(region string) string {
	if region == "" {
		return "us-central1-a"
	}
	return region + "-a"
}
