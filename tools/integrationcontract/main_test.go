package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixtureManifests is a miniature three-namespace layout: a Namespace declared
// twice (the one-file-install pattern), a workload that pulls env from a
// ConfigMap and a Secret, a Service, a PVC and RBAC.
const fixtureManifests = `
apiVersion: v1
kind: Namespace
metadata:
  name: jaiscloud
  labels:
    jaiscloud.io/role: substrate
    jaiscloud.io/substrate: aws
---
apiVersion: v1
kind: Namespace
metadata:
  name: jaiscloud-gcp
  labels:
    jaiscloud.io/role: substrate
    jaiscloud.io/substrate: gcp
---
apiVersion: v1
kind: Namespace
metadata:
  name: jaiscloud-gcp
  labels:
    jaiscloud.io/role: substrate
    jaiscloud.io/substrate: gcp
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: jaiscloud-config
  namespace: jaiscloud
data:
  JAISCLOUD_PORT: "4566"
  JAISCLOUD_REGION: us-east-1
---
apiVersion: v1
kind: Secret
metadata:
  name: jaiscloud-dsn
  namespace: jaiscloud
type: Opaque
stringData:
  dsn: postgres://example
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: jaiscloud-blobs-pvc
  namespace: jaiscloud
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaiscloud
  namespace: jaiscloud
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels: {app: jaiscloud}
  template:
    metadata:
      labels: {app: jaiscloud}
    spec:
      serviceAccountName: jaiscloud
      containers:
        - name: jaiscloud
          image: jaisraj/jaiscloud-aws:latest
          imagePullPolicy: IfNotPresent
          envFrom:
            - configMapRef: {name: jaiscloud-config}
          env:
            - name: JAISCLOUD_DSN
              valueFrom:
                secretKeyRef: {name: jaiscloud-dsn, key: dsn}
            - name: JAISCLOUD_INSTANCE_ID
              value: aws-substrate-1
          ports:
            - containerPort: 4566
          volumeMounts:
            - {name: blobs, mountPath: /data/blobs}
      volumes:
        - name: blobs
          persistentVolumeClaim: {claimName: jaiscloud-blobs-pvc}
---
apiVersion: v1
kind: Service
metadata:
  name: jaiscloud
  namespace: jaiscloud
spec:
  type: LoadBalancer
  ports:
    - port: 4566
      targetPort: 4566
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: jaiscloud-executor
  namespace: jaiscloud
roleRef:
  kind: Role
  name: jaiscloud-executor
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: jaiscloud
    namespace: jaiscloud
`

const fixturePlaybook = `
version: 1
scenarios:
  inspect:
    steps:
      - run: deploy/deploy.sh --check --json
`

func writeFixtures(t *testing.T) (manifests, playbook string) {
	t.Helper()
	dir := t.TempDir()
	manifests = filepath.Join(dir, "manifests.yaml")
	playbook = filepath.Join(dir, "playbook.yaml")
	if err := os.WriteFile(manifests, []byte(fixtureManifests), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(playbook, []byte(fixturePlaybook), 0o644); err != nil {
		t.Fatal(err)
	}
	return manifests, playbook
}

func TestBuildContractExtractsManifestFacts(t *testing.T) {
	manifests, playbook := writeFixtures(t)
	c, err := buildContract([]string{manifests}, playbook)
	if err != nil {
		t.Fatalf("buildContract: %v", err)
	}

	if len(c.Topology.Namespaces) != 2 {
		t.Fatalf("namespaces = %+v, want 2 (the duplicate jaiscloud-gcp is collapsed)", c.Topology.Namespaces)
	}
	var aws *Namespace
	for i := range c.Topology.Namespaces {
		if c.Topology.Namespaces[i].Name == "jaiscloud" {
			aws = &c.Topology.Namespaces[i]
		}
	}
	if aws == nil || aws.Role != "substrate" || aws.Substrate != "aws" {
		t.Fatalf("aws namespace = %+v, want role=substrate substrate=aws", aws)
	}

	var dep *Object
	for i := range c.Topology.Objects {
		if c.Topology.Objects[i].Kind == "Deployment" {
			dep = &c.Topology.Objects[i]
		}
	}
	if dep == nil {
		t.Fatal("deployment missing from the contract")
	}
	if dep.Replicas == nil || *dep.Replicas != 1 {
		t.Errorf("replicas = %v, want 1", dep.Replicas)
	}
	if dep.Strategy != "Recreate" {
		t.Errorf("strategy = %q, want Recreate", dep.Strategy)
	}
	if dep.ServiceAccount != "jaiscloud" {
		t.Errorf("serviceAccount = %q, want jaiscloud", dep.ServiceAccount)
	}
	if len(dep.Claims) != 1 || dep.Claims[0] != "jaiscloud-blobs-pvc" {
		t.Errorf("claims = %v, want [jaiscloud-blobs-pvc]", dep.Claims)
	}
	if len(dep.Containers) != 1 {
		t.Fatalf("containers = %+v, want 1", dep.Containers)
	}
	ctr := dep.Containers[0]
	if ctr.Image != "jaisraj/jaiscloud-aws:latest" || ctr.PullPolicy != "IfNotPresent" {
		t.Errorf("container image/pull = %q/%q", ctr.Image, ctr.PullPolicy)
	}
	if len(ctr.Ports) != 1 || ctr.Ports[0].Port != 4566 {
		t.Errorf("container ports = %+v, want 4566", ctr.Ports)
	}
	if len(ctr.EnvFrom) != 1 || ctr.EnvFrom[0] != "configMap:jaiscloud-config" {
		t.Errorf("envFrom = %v", ctr.EnvFrom)
	}

	// The convenience env table merges envFrom and keeps secret references
	// symbolic (never the value).
	var values map[string]string
	for _, g := range c.Env {
		if g.Workload == "jaiscloud" {
			values = g.Values
		}
	}
	if values["JAISCLOUD_PORT"] != "4566" || values["JAISCLOUD_REGION"] != "us-east-1" {
		t.Errorf("envFrom values not merged: %v", values)
	}
	if values["JAISCLOUD_INSTANCE_ID"] != "aws-substrate-1" {
		t.Errorf("explicit env lost: %v", values)
	}
	if values["JAISCLOUD_DSN"] != "<secret:jaiscloud-dsn/dsn>" {
		t.Errorf("secret ref = %q, want a symbolic reference", values["JAISCLOUD_DSN"])
	}

	// Secret keys are listed; the value is not.
	for _, o := range c.Topology.Objects {
		if o.Kind == "Secret" {
			if len(o.Keys) != 1 || o.Keys[0] != "dsn" {
				t.Errorf("secret keys = %v, want [dsn]", o.Keys)
			}
		}
	}

	// Convenience image list.
	if len(c.Images) != 1 || c.Images[0].Workload != "jaiscloud" {
		t.Errorf("images = %+v", c.Images)
	}
	if _, ok := c.Playbook["scenarios"]; !ok {
		t.Errorf("playbook not embedded: %+v", c.Playbook)
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	manifests, playbook := writeFixtures(t)
	a, err := buildContract([]string{manifests}, playbook)
	if err != nil {
		t.Fatal(err)
	}
	b, err := buildContract([]string{manifests}, playbook)
	if err != nil {
		t.Fatal(err)
	}
	ra, err := render(a)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := render(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(ra) != string(rb) {
		t.Fatal("render is not deterministic")
	}
	if !strings.HasPrefix(string(ra), "# GENERATED by") {
		t.Error("missing generated-file header")
	}
	if !strings.Contains(string(ra), "kind: IntegrationContract") {
		t.Error("missing contract kind")
	}
}

func TestSplitList(t *testing.T) {
	got := splitList(" a.yaml, ,b.yaml ")
	want := []string{"a.yaml", "b.yaml"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("splitList = %v, want %v", got, want)
	}
}
