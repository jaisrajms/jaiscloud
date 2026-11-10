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

// executionHistoryLevels are the documented values of
// workflows.v1.Workflow.executionHistoryLevel (the
// google.cloud.workflows.v1.ExecutionHistoryLevel enum). The proto's first
// value, EXECUTION_HISTORY_LEVEL_UNSPECIFIED (0), is the default and means
// "unset": the emulator stores it as "" so a read omits the field exactly as
// real GCP does (the differential golden for workflows.get omits it).
var executionHistoryLevels = []string{
	"EXECUTION_HISTORY_BASIC",
	"EXECUTION_HISTORY_DETAILED",
}

// validateExecutionHistoryLevel rejects a value outside the documented enum.
// Real GCP parses the field with proto3 JSON, which rejects an unknown enum
// name with INVALID_ARGUMENT, and the gRPC transport cannot even carry one —
// so the check lives in the shared core and both transports agree. An empty
// value and the proto default name both mean "unset" (stored as "").
func validateExecutionHistoryLevel(level string) error {
	switch level {
	case "", "EXECUTION_HISTORY_LEVEL_UNSPECIFIED":
		return nil
	}
	for _, ok := range executionHistoryLevels {
		if level == ok {
			return nil
		}
	}
	return invalidArgument("executionHistoryLevel must be one of EXECUTION_HISTORY_LEVEL_UNSPECIFIED, EXECUTION_HISTORY_BASIC, EXECUTION_HISTORY_DETAILED")
}

// normalizeExecutionHistoryLevel maps the proto default enum name onto the
// stored "unset" value, so an explicitly supplied
// EXECUTION_HISTORY_LEVEL_UNSPECIFIED is rendered the same way as an omitted
// field (proto3 omits a zero enum on read).
func normalizeExecutionHistoryLevel(level string) string {
	if level == "EXECUTION_HISTORY_LEVEL_UNSPECIFIED" {
		return ""
	}
	return level
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
