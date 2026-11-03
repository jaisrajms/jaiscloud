package container

import (
	"regexp"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// bootDiskKmsKeyForm is the CMEK key name Discovery documents for
// NodeConfig.bootDiskKmsKey: "projects/[KEY_PROJECT_ID]/locations/[LOCATION]/
// keyRings/[RING_NAME]/cryptoKeys/[KEY_NAME]". It is a resource name from any
// project (a Shared-VPC or cross-project key is legal), and "global" is a legal
// location.
var bootDiskKmsKeyForm = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

// validateNodeConfig checks the node-config fields GKE rejects before it would
// provision anything: a bootDiskKmsKey that is not a KMS crypto-key resource
// name, and an accelerator entry that names no type or asks for no card.
//
// Both are request-shape faults, not catalog lookups: the emulator has no GPU
// catalog to check an acceleratorType against (real GKE rejects an unknown one),
// so only the documented form of the field is enforced.
func validateNodeConfig(c *containerstore.NodeConfig) error {
	if c == nil {
		return nil
	}
	if c.BootDiskKmsKey != "" && !bootDiskKmsKeyForm.MatchString(c.BootDiskKmsKey) {
		return invalidArgument("nodeConfig.bootDiskKmsKey must be of the form projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}: " + c.BootDiskKmsKey)
	}
	for _, a := range c.Accelerators {
		if a.AcceleratorType == "" {
			return invalidArgument("nodeConfig.accelerators requires acceleratorType")
		}
		if a.AcceleratorCount <= 0 {
			return invalidArgument("nodeConfig.accelerators requires a positive acceleratorCount")
		}
	}
	return nil
}
