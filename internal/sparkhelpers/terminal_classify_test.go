package sparkhelpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/k8shelpers"
)

// buildTerminalPod creates a fake pod with the given phase and exit code.
func buildTerminalPod(jobName, ns string, phase corev1.PodPhase, exitCode int32, reason string) *corev1.Pod {
	terminated := &corev1.ContainerStateTerminated{
		ExitCode: exitCode,
		Reason:   reason,
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName + "-pod",
			Namespace: ns,
			Labels:    map[string]string{"job-name": jobName},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "spark-submit", Image: "spark:3.5"}},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "spark-submit",
					State: corev1.ContainerState{Terminated: terminated},
				},
			},
		},
	}
}

func TestWaitTerminal_Exit0_SparkSucceeded(t *testing.T) {
	jobName := "jc-spark-exit0"
	ns := "jaiscloud"

	pod := buildTerminalPod(jobName, ns, corev1.PodSucceeded, 0, "Completed")
	k8s := fake.NewSimpleClientset(pod)

	handle := k8shelpers.JobHandle{
		JobName:   jobName,
		Namespace: ns,
	}

	f, err := WaitTerminal(context.Background(), k8s, handle)
	require.NoError(t, err)
	assert.True(t, f.SparkSucceeded, "exit 0 should be SparkSucceeded=true")
}

func TestWaitTerminal_Exit143_WithSparkContextStopped(t *testing.T) {
	jobName := "jc-spark-exit143"
	ns := "jaiscloud"

	pod := buildTerminalPod(jobName, ns, corev1.PodFailed, 143, "Error")
	// Add log content to the pod via fake log (fake client doesn't support real logs,
	// so we test that the classification logic works when logs are empty — signal exit
	// without log marker should NOT be SparkSucceeded).
	k8s := fake.NewSimpleClientset(pod)

	handle := k8shelpers.JobHandle{
		JobName:   jobName,
		Namespace: ns,
	}

	f, err := WaitTerminal(context.Background(), k8s, handle)
	require.NoError(t, err)
	// Without the "SparkContext stopped successfully" marker in logs (fake client
	// returns empty logs), rule 2 does NOT fire → SparkSucceeded=false.
	assert.False(t, f.SparkSucceeded, "exit 143 without log marker should be SparkSucceeded=false")
}

func TestWaitTerminal_Exit1_SparkFailed(t *testing.T) {
	jobName := "jc-spark-exit1"
	ns := "jaiscloud"

	pod := buildTerminalPod(jobName, ns, corev1.PodFailed, 1, "Error")
	k8s := fake.NewSimpleClientset(pod)

	handle := k8shelpers.JobHandle{
		JobName:   jobName,
		Namespace: ns,
	}

	f, err := WaitTerminal(context.Background(), k8s, handle)
	require.NoError(t, err)
	assert.False(t, f.SparkSucceeded)
}

func TestWaitTerminal_OOMKilled_SparkFailed(t *testing.T) {
	jobName := "jc-spark-oom"
	ns := "jaiscloud"

	pod := buildTerminalPod(jobName, ns, corev1.PodFailed, 137, "OOMKilled")
	k8s := fake.NewSimpleClientset(pod)

	handle := k8shelpers.JobHandle{
		JobName:   jobName,
		Namespace: ns,
	}

	f, err := WaitTerminal(context.Background(), k8s, handle)
	require.NoError(t, err)
	assert.False(t, f.SparkSucceeded)
	assert.Equal(t, "OOMKilled", f.SparkReason)
}

// TestClassify_StrictExitCode_SQLFailure verifies that a Spark SQL CLI failure
// (non-zero exit, a clean "Shutdown hook called" tail, but no "ERROR" line) is
// reported as a failure when StrictExitCode is set — and would be
// misclassified as success by the lenient rule alone.
func TestClassify_StrictExitCode_SQLFailure(t *testing.T) {
	base := k8shelpers.Final{Succeeded: false, ExitCode: 1, Reason: "Error"}
	logs := []string{
		"24/01/01 12:00:00 INFO SparkContext: Running Spark",
		"org.apache.spark.sql.AnalysisException: Table or view not found: nope",
		"24/01/01 12:00:01 INFO ShutdownHookManager: Shutdown hook called",
	}

	lenient := Classify(base, logs, TerminalOptions{})
	assert.True(t, lenient.SparkSucceeded, "lenient rule 4 reports success")

	strict := Classify(base, logs, TerminalOptions{StrictExitCode: true})
	assert.False(t, strict.SparkSucceeded, "strict exit-code classification must report failure")
	assert.NotEmpty(t, strict.SparkReason)
}

// TestClassify_MainThreadExceptionFails verifies that an uncaught exception on
// the driver's main thread is a failure even when the SparkContext shut down
// cleanly and no "ERROR" line was printed (the lenient shutdown-hook rule would
// otherwise report success). It must fire with and without StrictExitCode.
func TestClassify_MainThreadExceptionFails(t *testing.T) {
	base := k8shelpers.Final{Succeeded: false, ExitCode: 1, Reason: "Error"}
	exception := `Exception in thread "main" java.lang.NumberFormatException: For input string: "not-a-number"`
	logs := []string{
		"24/01/01 12:00:00 INFO SparkContext: Running Spark",
		exception,
		"\tat org.apache.spark.examples.SparkPi$.main(SparkPi.scala:32)",
		"24/01/01 12:00:01 INFO SparkContext: Successfully stopped SparkContext",
		"24/01/01 12:00:01 INFO ShutdownHookManager: Shutdown hook called",
	}

	for _, opts := range []TerminalOptions{{}, {StrictExitCode: true}} {
		f := Classify(base, logs, opts)
		assert.False(t, f.SparkSucceeded, "uncaught main-thread exception must fail (opts %+v)", opts)
		assert.Contains(t, f.SparkReason, "NumberFormatException")
	}

	// Rule precedence: rule 1 (successful exit) is checked first, so a main
	// exception in the logs can never flip a successful driver to failure.
	exitedZero := Classify(k8shelpers.Final{Succeeded: true, ExitCode: 0}, logs, TerminalOptions{})
	assert.True(t, exitedZero.SparkSucceeded, "rule 1 (exit 0) wins over the main-exception rule")
	assert.Equal(t, "exit 0", exitedZero.SparkReason)

	// Rule precedence: rule 2 (signal exit + SparkContext stopped successfully)
	// is checked before the main-exception rule, so a cancelled-then-cleaned
	// driver still succeeds.
	signalLogs := append(append([]string{}, logs...), "INFO SparkContext: SparkContext stopped successfully")
	signalled := Classify(k8shelpers.Final{Succeeded: false, ExitCode: 143}, signalLogs, TerminalOptions{})
	assert.True(t, signalled.SparkSucceeded, "rule 2 (signal exit + stopped marker) wins over the main-exception rule")
	assert.Equal(t, "SparkContext stopped successfully", signalled.SparkReason)
}

// TestClassify_NonMainThreadExceptionStaysSuccess pins the main-thread scope: a
// clean shutdown-hook exit with an exception on another thread is still
// success under the lenient rule.
func TestClassify_NonMainThreadExceptionStaysSuccess(t *testing.T) {
	base := k8shelpers.Final{Succeeded: false, ExitCode: 1, Reason: "Error"}
	logs := []string{
		"24/01/01 12:00:00 INFO SparkContext: Running Spark",
		`Exception in thread "pool-1-thread-1" java.io.IOException: connection reset`,
		"24/01/01 12:00:01 INFO ShutdownHookManager: Shutdown hook called",
	}

	f := Classify(base, logs, TerminalOptions{})
	assert.True(t, f.SparkSucceeded, "a non-main-thread exception plus a clean shutdown hook stays success")
	assert.Equal(t, "Shutdown hook called (no errors in tail)", f.SparkReason)
}

// TestLastMainThreadExceptionLine verifies the helper is tail-scoped and only
// matches the main thread.
func TestLastMainThreadExceptionLine(t *testing.T) {
	assert.Equal(t,
		`Exception in thread "main" java.lang.RuntimeException: boom`,
		lastMainThreadExceptionLine([]string{
			`Exception in thread "main" java.lang.IllegalStateException: old`,
			"INFO: done",
			`Exception in thread "main" java.lang.RuntimeException: boom`,
		}, 200))

	assert.Equal(t, "", lastMainThreadExceptionLine([]string{
		`Exception in thread "pool-1-thread-1" java.io.IOException: nope`,
	}, 200))

	// Outside the tail window: ignored.
	lines := make([]string, 300)
	for i := range lines {
		lines[i] = "INFO: line"
	}
	lines[10] = `Exception in thread "main" java.lang.IllegalStateException: stale`
	assert.Equal(t, "", lastMainThreadExceptionLine(lines, 200))
}

// TestClassificationHelpers tests internal helpers used for log-based classification.
func TestContainsLine(t *testing.T) {
	lines := []string{
		"24/01/01 12:00:00 INFO SparkContext: SparkContext stopped successfully",
		"24/01/01 12:00:01 INFO Application finished",
	}
	assert.True(t, containsLine(lines, "SparkContext stopped successfully"))
	assert.False(t, containsLine(lines, "ERROR"))
}

func TestHasErrorInTail(t *testing.T) {
	lines := make([]string, 300)
	for i := range lines {
		lines[i] = "INFO: line"
	}
	// Put an ERROR in position 250 (in last 200).
	lines[250] = "ERROR: something went wrong"
	assert.True(t, hasErrorInTail(lines, 200))

	// Put ERROR only in first 50 lines.
	lines[250] = "INFO: line"
	lines[10] = "ERROR: early failure"
	assert.False(t, hasErrorInTail(lines, 200))
}

func TestLastErrorLine(t *testing.T) {
	lines := []string{
		"ERROR: first error",
		"INFO: something",
		"ERROR: last error",
		"INFO: done",
	}
	assert.Equal(t, "ERROR: last error", lastErrorLine(lines))

	noErrors := []string{"INFO: a", "INFO: b"}
	assert.Equal(t, "", lastErrorLine(noErrors))
}
