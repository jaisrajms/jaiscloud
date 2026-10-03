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

	lenient := classify(base, logs, TerminalOptions{})
	assert.True(t, lenient.SparkSucceeded, "lenient rule 3 reports success")

	strict := classify(base, logs, TerminalOptions{StrictExitCode: true})
	assert.False(t, strict.SparkSucceeded, "strict exit-code classification must report failure")
	assert.NotEmpty(t, strict.SparkReason)
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
