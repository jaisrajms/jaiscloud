package workflows

import (
	"strings"

	workflowengine "jaiscloud/internal/gcp/workflows/engine"
)

// This file holds the workflows.create/update input validation real Cloud
// Workflows performs before it accepts a workflow, which the emulator used to
// skip: an invalid definition or an invalid workflowId was accepted and only
// surfaced later (a bad definition failed at execution time; a bad id was
// stored verbatim). Both are now INVALID_ARGUMENT at the management-plane call.

// validateWorkflowID enforces the documented workflowId contract (Discovery,
// workflows.projects.locations.workflows.create):
//
//   - only letters, numbers, underscores and hyphens;
//   - must start with a letter;
//   - must be between 1 and 64 characters;
//   - must end with a number or a letter.
//
// An empty id is handled separately by the caller (it means "derive from the
// resource name" on update).
func validateWorkflowID(id string) error {
	if id == "" {
		return invalidArgument("missing workflowId")
	}
	if len(id) > 64 {
		return invalidArgument("workflowId must be at most 64 characters")
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := c == '_' || c == '-' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !ok {
			return invalidArgument("workflowId " + id + " contains an invalid character (only letters, numbers, underscores and hyphens are allowed)")
		}
	}
	if !isLetter(id[0]) {
		return invalidArgument("workflowId " + id + " must start with a letter")
	}
	if last := id[len(id)-1]; !isLetter(last) && (last < '0' || last > '9') {
		return invalidArgument("workflowId " + id + " must end with a number or a letter")
	}
	return nil
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// validateSource rejects a malformed workflow definition with the same
// structural validation the engine applies before executing, so create/update
// fail loud at the management-plane call instead of deferring the error to the
// first execution. A definition is required (real Cloud Workflows requires
// sourceContents or sourceCode; the emulator models only sourceContents), and
// it must fit the documented 128KB limit.
func validateSource(source string) error {
	if strings.TrimSpace(source) == "" {
		return invalidArgument("workflow sourceContents is required")
	}
	if len(source) > 128*1024 {
		return invalidArgument("workflow sourceContents must be at most 128KB")
	}
	if err := workflowengine.Validate(source); err != nil {
		return invalidArgument("invalid workflow definition: " + err.Error())
	}
	return nil
}
