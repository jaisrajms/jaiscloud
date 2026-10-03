package sparkhelpers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
)

const (
	defaultMaster         = "k8s://kubernetes.default.svc"
	executorTemplateMount = "/jaiscloud/spark"
	executorTemplateKey   = "executor-template.yaml"
	executorTemplatePath  = "/jaiscloud/spark/executor-template.yaml"
)

func nonEmptyOr(s, def string) string {
	if s != "" {
		return s
	}
	return def
}

// driverCommand returns the driver container's binary. A SqlEntryPoint runs the
// spark-sql CLI (its `-e`/`-f` flags are parsed by the SQL driver, not by
// spark-submit), defaulting to a `spark-sql` sibling of SparkSubmitPath so a
// deployment that sets the off-PATH /opt/spark/bin/spark-submit gets
// /opt/spark/bin/spark-sql without a second setting. Every other entry point
// runs spark-submit.
func driverCommand(job ClientModeJob) string {
	if _, ok := job.EntryPoint.(SqlEntryPoint); ok {
		if job.SparkSqlPath != "" {
			return job.SparkSqlPath
		}
		return path.Join(path.Dir(job.SparkSubmitPath), "spark-sql")
	}
	return nonEmptyOr(job.SparkSubmitPath, "spark-submit")
}

// sanitizeJobID lowercases the jobID, replaces non-alphanumeric chars with '-',
// and truncates to 52 chars (to stay within k8s 63-char limit with prefix).
func sanitizeJobID(jobID string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(jobID) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	s := sb.String()
	// Collapse consecutive dashes and trim leading/trailing dashes.
	re := regexp.MustCompile(`-+`)
	s = re.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 52 {
		s = s[:52]
		s = strings.TrimRight(s, "-")
	}
	return s
}

// BuildClientModeArgs returns the argv for the driver tool (excluding the binary
// name itself). It is spark-submit's argv for jar/python/R jobs, and the
// spark-sql CLI's argv for a SqlEntryPoint (spark-sql forwards the standard
// spark-submit flags to spark-submit and the SQL flags to the SQL driver).
// Argv order:
//  1. --master k8s://kubernetes.default.svc
//  2. --deploy-mode client
//  3. --conf spark.kubernetes.namespace=<job.Namespace>
//  4. --conf spark.kubernetes.driver.pod.name=$(SPARK_DRIVER_POD_NAME)
//  5. --conf spark.kubernetes.executor.podTemplateFile=file:///jaiscloud/spark/executor-template.yaml
//  6. --conf spark.driver.bindAddress=0.0.0.0
//  7. caller's SparkSubmitArgs verbatim
//  8. entry-point pre-args (e.g. --class MainClass, or the SQL --jars/-e/-f)
//  9. jarOrScript (empty for SQL)
//  10. JarArgs
func BuildClientModeArgs(job ClientModeJob) []string {
	args := []string{
		"--master", defaultMaster,
		"--deploy-mode", "client",
		"--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", job.Namespace),
		"--conf", "spark.kubernetes.driver.pod.name=$(SPARK_DRIVER_POD_NAME)",
		"--conf", fmt.Sprintf("spark.kubernetes.executor.podTemplateFile=file://%s", executorTemplatePath),
		"--conf", "spark.kubernetes.executor.podTemplateContainerName=spark-kubernetes-executor",
		"--conf", fmt.Sprintf("spark.kubernetes.container.image=%s", job.Image),
		"--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
		"--conf", "spark.kubernetes.authenticate.executor.serviceAccountName=" + nonEmptyOr(job.ServiceAccountName, "default"),
		"--conf", "spark.driver.bindAddress=0.0.0.0",
		"--conf", "spark.driver.host=$(SPARK_DRIVER_BIND_ADDRESS)",
		"--conf", "spark.kubernetes.driver.request.cores=200m",
		"--conf", "spark.kubernetes.driver.limit.cores=1000m",
		"--conf", "spark.kubernetes.executor.request.cores=200m",
		"--conf", "spark.kubernetes.executor.limit.cores=1000m", // Minimal executor CPU limit to avoid scheduling issues.
	}

	// Cloud-specific confs prepended so caller's SparkSubmitArgs win (last-value-wins).
	args = append(args, job.ExtraSparkConfs...)

	// Caller's extra spark-submit args.
	args = append(args, job.SparkSubmitArgs...)

	// Entry-point args.
	if job.EntryPoint != nil {
		preJarArgs, jarOrScript := EntryPointArgs(job.EntryPoint)
		args = append(args, preJarArgs...)
		if jarOrScript != "" {
			args = append(args, jarOrScript)
		}
	}

	// Positional jar/script args.
	args = append(args, job.JarArgs...)

	return args
}

// SubmitClientMode builds and submits a spark-submit k8s Job in client mode.
// Steps:
//  1. Validates required fields (Image)
//  2. Builds executor pod template via BuildExecutorPodTemplate
//  3. Creates ConfigMap spark-exec-tpl-<sanitized-jobID> with template YAML
//  4. Builds main container (spark-submit, or spark-sql for a SqlEntryPoint) with driver resources
//  5. Mounts ConfigMap via projected volume
//  6. Calls k8shelpers.BuildPodSpec for platform overlay + driver template
//  7. Calls k8shelpers.SubmitJob and returns the handle
func SubmitClientMode(ctx context.Context, k8s kubernetes.Interface, job ClientModeJob) (k8shelpers.JobHandle, error) {
	if job.Image == "" {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: ClientModeJob.Image is required")
	}
	// 1. Build executor pod template YAML.
	execTplYAML, err := BuildExecutorPodTemplate(ctx, k8s, job.PlatformOverlay, job.CallerExecutorPodTpl)
	if err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: build executor pod template: %w", err)
	}

	// 2. Create ConfigMap for executor template. On a restart attempt the name
	// is suffixed so it does not collide with the previous attempt's objects.
	objectSuffix := ""
	if job.Attempt > 0 {
		objectSuffix = fmt.Sprintf("-r%d", job.Attempt)
	}
	cmName := "spark-exec-tpl-" + sanitizeJobID(job.JobID) + objectSuffix
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: job.Namespace,
			Labels:    job.Labels,
		},
		Data: map[string]string{
			executorTemplateKey: string(execTplYAML),
		},
	}
	if job.OwnerHint != nil {
		isController := true
		cm.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: job.OwnerHint.APIVersion,
			Kind:       job.OwnerHint.Kind,
			Name:       job.OwnerHint.Name,
			UID:        job.OwnerHint.UID,
			Controller: &isController,
		}}
	}
	if _, err := k8s.CoreV1().ConfigMaps(job.Namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: create executor template configmap: %w", err)
	}
	// cmSubmitOK is set to true only after SubmitJob succeeds. Until then, the
	// deferred cleanup deletes the orphaned ConfigMap on any error path.
	cmSubmitOK := false
	defer func() {
		if cmSubmitOK {
			return
		}
		_ = k8s.CoreV1().ConfigMaps(job.Namespace).Delete(context.Background(), cmName, metav1.DeleteOptions{})
	}()

	// 3. Build the driver binary path: spark-sql for a SQL entry point (the SQL
	// CLI flags are not spark-submit flags), spark-submit otherwise.
	sparkSubmitPath := driverCommand(job)

	// Build main container.
	mainContainer := corev1.Container{
		Name:            "spark-submit",
		Image:           job.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{sparkSubmitPath},
		Args:            BuildClientModeArgs(job),
		Env: append([]corev1.EnvVar{
			{
				Name: "SPARK_DRIVER_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			{
				Name: "SPARK_DRIVER_BIND_ADDRESS",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
				},
			},
		}, job.ExtraDriverEnv...),
	}

	// Set driver resources if specified.
	if job.DriverResources.CPU != "" || job.DriverResources.Memory != "" {
		reqs := corev1.ResourceList{}
		lims := corev1.ResourceList{}
		if job.DriverResources.CPU != "" {
			q := resource.MustParse(job.DriverResources.CPU)
			reqs[corev1.ResourceCPU] = q
			lims[corev1.ResourceCPU] = q
		}
		if job.DriverResources.Memory != "" {
			q := resource.MustParse(job.DriverResources.Memory)
			reqs[corev1.ResourceMemory] = q
			lims[corev1.ResourceMemory] = q
		}
		mainContainer.Resources = corev1.ResourceRequirements{
			Requests: reqs,
			Limits:   lims,
		}
	}

	// 4. Mount ConfigMap via projected volume.
	volumeName := "spark-exec-tpl"
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: executorTemplateMount,
	})

	// 5. Build pod spec via k8shelpers.BuildPodSpec.
	base := k8shelpers.PodSpecInput{
		MainContainer: mainContainer,
		Namespace:     job.Namespace,
		Labels:        job.Labels,
		Annotations:   job.Annotations,
	}

	tpl, err := k8shelpers.BuildPodSpec(ctx, k8s, base, job.PlatformOverlay, job.CallerDriverPodTpl, job.IdentityMutator)
	if err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: build pod spec: %w", err)
	}

	// IdentityMutator wins; only fill ServiceAccountName if still empty.
	if tpl.Spec.ServiceAccountName == "" {
		tpl.Spec.ServiceAccountName = job.ServiceAccountName
	}

	// Add projected volume for executor template ConfigMap.
	tpl.Spec.Volumes = append(tpl.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{
					{
						ConfigMap: &corev1.ConfigMapProjection{
							LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
							Items: []corev1.KeyToPath{
								{
									Key:  executorTemplateKey,
									Path: executorTemplateKey,
								},
							},
						},
					},
				},
			},
		},
	})

	// 6. Submit the job.
	jobName := "jc-spark-cm-" + sanitizeJobID(job.JobID) + objectSuffix
	req := k8shelpers.SubmitJobRequest{
		Namespace:               job.Namespace,
		JobName:                 jobName,
		Spec:                    tpl.Spec,
		Labels:                  job.Labels,
		Annotations:             job.Annotations,
		Parallelism:             1,
		BackoffLimit:            0,
		TTLSecondsAfterFinished: job.TTLSecondsAfterFinished,
		OwnerRef:                job.OwnerHint,
	}

	handle, err := k8shelpers.SubmitJob(ctx, k8s, req)
	if err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: submit job: %w", err)
	}
	// Job created — patch CM owner so K8s GC cleans it up when the Job is GC'd.
	// This is necessary when job.OwnerHint is nil (the common case for EMR steps),
	// since the CM was created without ownerReferences in that path.
	if err := patchCMOwner(ctx, k8s, job.Namespace, cmName, handle.JobName, handle.JobUID); err != nil {
		slog.Warn("sparkhelpers: failed to set CM ownerReference — CM will leak if Job is GC'd",
			"cm", cmName, "job", handle.JobName, "err", err)
	}
	cmSubmitOK = true
	return handle, nil
}

// patchCMOwner patches a ConfigMap to set its ownerReferences to a batch/v1 Job.
// This ensures the CM is garbage-collected by K8s when the Job is deleted.
func patchCMOwner(ctx context.Context, k8s kubernetes.Interface, namespace, cmName, jobName string, jobUID k8stypes.UID) error {
	isController := true
	owner := metav1.OwnerReference{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Name:       jobName,
		UID:        jobUID,
		Controller: &isController,
	}
	ownerJSON, err := json.Marshal([]metav1.OwnerReference{owner})
	if err != nil {
		return err
	}
	patch := fmt.Sprintf(`{"metadata":{"ownerReferences":%s}}`, string(ownerJSON))
	_, err = k8s.CoreV1().ConfigMaps(namespace).Patch(ctx, cmName, k8stypes.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}
