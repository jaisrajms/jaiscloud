package sparkgcp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBuildWorkloadIdentityMutator_CreatesServiceAccount(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	m := BuildWorkloadIdentityMutator("jaiscloud", "my-sa", "my-proj")
	tpl := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}}}
	require.NoError(t, m(context.Background(), k8s, tpl))

	_, err := k8s.CoreV1().ServiceAccounts("jaiscloud").Get(context.Background(), "my-sa", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "my-sa", tpl.Spec.ServiceAccountName)
	require.Contains(t, tpl.Spec.Containers[0].Env, corev1.EnvVar{Name: "GOOGLE_CLOUD_PROJECT", Value: "my-proj"})
}

func TestBuildWorkloadIdentityMutator_ExistingSAIdempotent(t *testing.T) {
	k8s := fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "jaiscloud"},
	})
	m := BuildWorkloadIdentityMutator("jaiscloud", "sa", "p")
	tpl := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}}}
	require.NoError(t, m(context.Background(), k8s, tpl))

	sas, _ := k8s.CoreV1().ServiceAccounts("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.Len(t, sas.Items, 1)
}

func TestBuildWorkloadIdentityMutator_CallerWinsOnSAName(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	m := BuildWorkloadIdentityMutator("jaiscloud", "vc-sa", "p")
	tpl := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			ServiceAccountName: "caller-sa",
			Containers:         []corev1.Container{{Name: "spark-submit"}},
		},
	}
	require.NoError(t, m(context.Background(), k8s, tpl))
	require.Equal(t, "caller-sa", tpl.Spec.ServiceAccountName)
	_, err := k8s.CoreV1().ServiceAccounts("jaiscloud").Get(context.Background(), "caller-sa", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestBuildWorkloadIdentityMutator_EmptySkipsKubeCalls(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	m := BuildWorkloadIdentityMutator("jaiscloud", "", "")
	tpl := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}}}
	require.NoError(t, m(context.Background(), k8s, tpl))
	require.Empty(t, tpl.Spec.ServiceAccountName)
	sas, _ := k8s.CoreV1().ServiceAccounts("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.Empty(t, sas.Items)
}
