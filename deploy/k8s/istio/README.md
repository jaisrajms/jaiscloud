# Istio front door (dual-substrate deployment)

One Istio ingress gateway terminates TLS for the **real cloud hostnames** and
routes each one to the substrate that owns it. Clients change nothing: an AWS
SDK pointed at `https://sts.amazonaws.com`, a GCS client pointed at
`https://storage.googleapis.com`, a Firestore gRPC client on
`firestore.googleapis.com:443` and a Cloud Run app on
`https://svc-abc123.us-central1.run.app` all reach the same pair of emulators
through port 443 of one gateway.

The mesh is **only the client-facing front door**. Nothing inside a substrate
transits it: Spark executors, Cloud Run revision pods, function invocation pods
and the Managed Kafka broker talk to the emulator over in-cluster Service DNS
(`jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8080`).

```
                        +--------------------------------------+
   client               |  istio-system                        |
   ------               |  istio-ingressgateway                |
   sts.amazonaws.com -->|   :80  --301--> https                |
   storage.googleapis.com--> :443 TLS (jaiscloud-wildcard)     |
   firestore.googleapis.com:443 | Gateway jaiscloud            |
   svc-x.run.app ------>|   VirtualServices (no authority     |
                        |   rewrite anywhere)                 |
                        +----+-------------------------+-------+
                             |                         |
        *.amazonaws.com      |                         |  *.googleapis.com
        4566, no split       |                         |  (application/grpc -> 8081)
                             v                         v
              +----------------------+     +-------------------------------+
              | ns jaiscloud         |     | ns jaiscloud-gcp              |
              | svc jaiscloud:4566   |     | svc jaiscloud-gcp:8080 REST   |
              | (AWS substrate)      |     |                   :8081 gRPC  |
              +----------+-----------+     | *.run.app, *.cloudfunctions.net
                         |                 | -> :8080 (same VirtualService) |
                         |                 +---------------+---------------+
                         +----------> ns jaiscloud-data: postgres <--+
                                      (shared store, one DB per cloud)
```

## Which hosts each substrate owns

| Host pattern | Substrate | Service:port | Notes |
|---|---|---|---|
| `*.amazonaws.com` | AWS (`ns jaiscloud`) | `jaiscloud:4566` | one port for every AWS service: S3 (incl. virtual-hosted `bucket.s3.us-east-1.amazonaws.com`), STS, SQS, SNS, DynamoDB, IAM, Lambda, EKS, EMR, Glue, execute-api, … |
| `*.googleapis.com` | GCP (`ns jaiscloud-gcp`) | `8081` for gRPC, `8080` for everything else | the split is by `content-type: application/grpc` — see below |
| `*.run.app` | GCP | `jaiscloud-gcp:8080` | Cloud Run data plane, `{id}-{token}.{location}.run.app` |
| `*.cloudfunctions.net` | GCP | `jaiscloud-gcp:8080` | function trigger plane, `{location}-{project}.cloudfunctions.net` |

Not exposed through the gateway, by design:

- **Hive Metastore `9083..9093`** (Thrift, in-cluster only — Spark drivers reach
  it on the Service's own ports).
- Anything an executor or a managed workload calls back into; that traffic stays
  on Service DNS inside the cluster.

Gateway listening ports: **80** (redirects to HTTPS) and **443** (TLS, SNI-based
virtual hosting over the four wildcards). The emulators themselves stay
plaintext; TLS terminates at the gateway, so `JAISCLOUD_PLATFORM_TLS_*` keeps
its existing, narrower meaning: CA injection into *managed* workloads, not
listener TLS.

## Install order

Everything below is idempotent; run from the repo root.

1. **Namespaces and substrates** (owned by the deploy path, not this directory):

   ```bash
   kubectl apply -f deploy/k8s/namespace-data.yaml     # the shared store
   kubectl apply -f deploy/k8s/postgres.yaml
   kubectl apply -f deploy/k8s/namespace-aws.yaml      # only the substrate(s) you run
   kubectl apply -f deploy/k8s/namespace-gcp.yaml
   # ... plus rbac-shared.yaml and the substrate's rbac-<cloud>.yaml, then the
   # substrate manifests that create svc/jaiscloud and svc/jaiscloud-gcp
   ```

   `deploy/deploy.sh` also applies
   `deploy/k8s/rbac-admission-policy.yaml` (the namespace-isolation
   ValidatingAdmissionPolicy) before these, on every install path.

   The gateway only routes; it does not create backends. `jaiscloud-gcp` must
   expose **`8081` as a gRPC (h2c) port** — see "The gRPC route" below.

2. **Istio control plane + ingress gateway.**

   The `minimal` profile installs **istiod only — no gateway**. There is nothing
   to mesh here (no sidecars, no mTLS between substrates: the mesh is a front
   door), so the control plane stays minimal and we add exactly one thing to it:
   the ingress gateway our `Gateway`/`VirtualService` resources select. The
   alternative, `profile=default`, would also install an **egress** gateway — a
   second load balancer that no rule in this directory needs and that would make
   it ambiguous which gateway carries cloud traffic.

   ```bash
   istioctl install -y \
     --set profile=minimal \
     --set 'components.ingressGateways[0].enabled=true' \
     --set 'components.ingressGateways[0].name=istio-ingressgateway' \
     --set 'components.ingressGateways[0].k8s.service.type=LoadBalancer'
   kubectl -n istio-system rollout status deploy/istiod deploy/istio-ingressgateway
   kubectl -n istio-system get svc istio-ingressgateway   # must show 80 and 443
   ```

   Validated with `istioctl` 1.32.0 on k3d: the install brings up `istiod` and
   `istio-ingressgateway`, and the Service reports `80:…/TCP,443:…/TCP`
   (`443` targets the proxy's `8443`).

   **Do not run this on a cluster that already has Istio.** `istioctl install`
   is a full install request: with an existing istiod it can replace that
   cluster's IstioOperator configuration and `meshConfig` and prune components
   other teams rely on. See "Existing Istio install" below.

   `istioctl install` also installs the Istio CRDs (`Gateway`,
   `VirtualService`, `DestinationRule`, …), so no separate CRD step is needed —
   but the CRDs must be present *before* `kubectl apply -f
   deploy/k8s/istio/*.yaml`, otherwise those objects are rejected as unknown
   kinds.

   The gateway Service's `443` port maps to the proxy's `8443`; the gateway
   container also listens on `8080` behind the Service's `80`. Those are Istio's
   own port numbers — our `Gateway` resource refers only to `80`/`443`.

   On k3d/k3s ServiceLB and on Docker Desktop the LoadBalancer gets an address
   and terminates on 443 there. **kind has no LoadBalancer controller**, so
   `svc/istio-ingressgateway` stays `<pending>`: either install a small LB
   (MetalLB) or use the Service's NodePort — `scripts/istio-smoke.sh` discovers
   the `443` NodePort on the node address automatically, and
   `SMOKE_PORT_FORWARD=1` uses `kubectl port-forward` instead. `extraPortMappings`
   alone does not help (it maps a host port to a node/container port, not to a
   Service's LoadBalancer IP).

3. **Dev CA + wildcard certificate + CA ConfigMaps**:

   ```bash
   deploy/k8s/istio/certs/gen.sh
   ```

   Creates a dev CA in `${XDG_CACHE_HOME:-$HOME/.cache}/jaiscloud/istio-ca`
   (never in the repo), a leaf covering `*.amazonaws.com`, `*.googleapis.com`,
   `*.run.app`, `*.cloudfunctions.net` plus the deeper names X.509 wildcards
   cannot reach:
   `*.{location}.run.app`, `*.execute-api.{region}.amazonaws.com`,
   `*.{region}.amazonaws.com` and `*.s3.{region}.amazonaws.com`, then:

   Why the AWS depth matters: `*.amazonaws.com` is a **single-label** wildcard,
   so it covers `sts.amazonaws.com` but not `s3.us-east-1.amazonaws.com`
   (three labels) or `bucket.s3.us-east-1.amazonaws.com` (four). Every AWS SDK
   defaults to a regional endpoint, so without the regional SANs the front door
   would route correctly but fail client-side certificate validation. Add
   regions with `AWS_REGIONS="us-east-1 eu-west-1"` and locations with
   `RUN_APP_LOCATIONS`; `gen.sh` self-checks every claimed depth with
   `openssl -checkhost` and refuses to apply a leaf that misses one.

   - Secret `istio-system/jaiscloud-wildcard` (`tls.crt`, `tls.key`, `ca.crt`);
   - ConfigMap `jaiscloud-ca-cert` (key `ca.crt`) in **`jaiscloud`** and
     **`jaiscloud-gcp`** — or in whatever names the install uses: `gen.sh` reads
     `NS_AWS`/`NS_GCP` (the same variables `deploy/deploy.sh` reads) and places
     the copies there. With a custom layout:
     `NS_AWS=team-aws NS_GCP=team-gcp deploy/k8s/istio/certs/gen.sh`, or pass
     `CM_NAMESPACES="$NS_AWS $NS_GCP"` explicitly.

   The ConfigMap shape is exactly what the platform TLS code already consumes —
   `internal/platform/config.go` defaults `JAISCLOUD_PLATFORM_TLS_CA_SOURCES` to
   `CASource{Name: "jaiscloud", Source: CASourceRef{Kind: "configMap", Name:
   "jaiscloud-ca-cert", Key: "ca.crt"}}`, so a workload that sets
   `JAISCLOUD_PLATFORM_TLS_ENABLED=true` imports it without any extra env. The
   shipped substrate manifests keep platform TLS **disabled**: they talk to the
   emulator over plaintext in-cluster Service DNS and TLS termination is the
   gateway's job, so no CA is needed on that path. The ConfigMap is for
   workloads you point at the front door instead. `gen.sh` only *writes* the
   objects: it never sets that variable and never edits a manifest.

   Re-running is a no-op; `--force` regenerates the CA, and then every client
   that trusted the old `ca.crt` must re-import it and running pods must be
   restarted. `--generate-only` produces the files without touching the cluster.

4. **Route configuration**:

   ```bash
   kubectl apply -f deploy/k8s/istio/gateway.yaml
   kubectl apply -f deploy/k8s/istio/virtualservices.yaml
   kubectl apply -f deploy/k8s/istio/destinationrule.yaml
   ```

   The two route files address each substrate by a fully-qualified Service name,
   so a custom namespace layout needs those FQDNs rewritten. `render.sh` does
   exactly that from `NS_AWS`/`NS_GCP`, and renders the shipped names when they
   are unset (so it is a safe replacement for the two `kubectl apply -f` lines):

   ```bash
   deploy/k8s/istio/render.sh --plan     # print the rendered manifests
   NS_AWS=team-aws NS_GCP=team-gcp deploy/k8s/istio/render.sh --apply
   ```

   Then verify the mesh picks the configuration up cleanly:

   ```bash
   istioctl analyze -n istio-system          # expect: "No validation issues found"
   istioctl proxy-status                     # ingressgateway: all types subscribed, none STALE
   ```

   `istioctl analyze` must report **no warnings and no errors**. The one
   expected exception is `IST0101 Referenced host not found` for
   `jaiscloud.jaiscloud.svc.cluster.local` and
   `jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local` while the substrate Services
   of step 1 do not exist yet — that finding belongs to the deploy path, not to
   this directory, and clears as soon as those Services do. Anything else is a
   real misconfiguration.

   `deploy/k8s/istio/dns.yaml` is **documentation only** — it contains no
   Kubernetes object and is deliberately not applied (see "Cluster DNS").

5. **Prove it**:

   ```bash
   scripts/istio-smoke.sh
   ```

### Custom namespace layout

`deploy/deploy.sh` installs a custom namespace/ServiceAccount layout without
hand edits (`NS_AWS`, `NS_GCP`, `NS_DATA`, `SA_AWS`, `SA_GCP`; see
`deploy/deploy.sh --help`). The mesh follows the namespaces with two steps, both
keyed on the same variables:

```bash
# 1. CA ConfigMap copies into the substrate namespaces
NS_AWS=team-aws NS_GCP=team-gcp deploy/k8s/istio/certs/gen.sh

# 2. Route FQDNs: jaiscloud.<NS_AWS>.svc… / jaiscloud-gcp.<NS_GCP>.svc…
NS_AWS=team-aws NS_GCP=team-gcp deploy/k8s/istio/render.sh --apply
```

Nothing else in this directory is namespace-dependent: the gateway lives in
`istio-system`, routes by host, and never rewrites `:authority`. The checked-in
`virtualservices.yaml` / `destinationrule.yaml` keep the default FQDNs, and
`render.sh` reproduces them byte-for-byte when the variables are unset.

### Existing Istio install

When `istiod` is already running (another application's mesh, a platform
install), add only the gateway JaisCloud needs — do **not** re-run
`istioctl install --set profile=minimal`:

```bash
# merge with the cluster's existing operator configuration when you have it
istioctl install -f <existing-operator.yaml> -f deploy/k8s/istio/istiooperator-gateway-only.yaml

# no existing operator file: render the standalone gateway and apply it
istioctl manifest generate -f deploy/k8s/istio/istiooperator-gateway-only.yaml \
  | kubectl apply -f -
kubectl -n istio-system rollout status deploy/istio-ingressgateway
```

[`istiooperator-gateway-only.yaml`](istiooperator-gateway-only.yaml) is an
`IstioOperator` with `profile: empty` that enables **only** the
`istio-ingressgateway` component (`LoadBalancer`, Istio's default 80/443). It
renders no control plane. `deploy/deploy.sh --check` warns whenever `istiod`
exists (`mesh.existingInstall` in the JSON) and names this overlay.

Be aware of the ports: the gateway's `LoadBalancer` binds **80 and 443**, which
competes with any other gateway on those ports — on Docker Desktop a
LoadBalancer binds the host directly, so a second gateway wanting 443
(`localhost:443`) cannot be published at the same time. Either stop the other
gateway or give this one distinct ports.

Changing the gateway name, namespace or ports is a coordinated edit:

- the `Gateway` resource (`gateway.yaml`: `metadata.name` / `metadata.namespace`)
  and its `selector` (`istio: ingressgateway`, the label the gateway Deployment
  carries — a second gateway installed under another name needs its own label and
  its own `Gateway` resource);
- the `VirtualServices`' `spec.gateways` reference, which must name the same
  `namespace/name` (the shipped files use `istio-system/jaiscloud`);
- the `Gateway.spec.servers[].port.number` values, and anything that points
  clients at 80/443 (the smoke script discovers the Service's ports).

A mismatch is silent in the worst way: Istio accepts a `VirtualService` that
references a nonexistent gateway and simply never programs the route.

## Never rewrite authority

**No `rewrite.authority`, no `host_rewrite`, no `Host` header mutation anywhere
in this directory, and none may be added.** The emulator's router takes the
request authority as input:

| surface | why the authority matters |
|---|---|
| `/v1/projects/{p}/locations/{l}/clusters` | **GKE and Managed Kafka both serve this exact path.** Only the `container` / `managedkafka` host label (plus a store probe) tells them apart (`internal/gcp/adapter/router.go`). A rewritten authority silently answers one service with the other's data. |
| `{id}-{token}.{location}.run.app` | Cloud Run data plane: the host carries the revision |
| `{location}-{project}.cloudfunctions.net` | function trigger: the host carries project + location |
| `bucket.s3.<region>.amazonaws.com` | S3 virtual-hosted style: the bucket is the first label |
| `{api}.execute-api.{region}.amazonaws.com` | API Gateway invoke plane: the host carries the API id |

Envoy forwards `:authority` unchanged unless it is told otherwise, so the mesh
is a TLS terminator + router, not a translator. `scripts/istio-smoke.sh`
assertion 1 exists only to catch a regression of this rule.

## The gRPC route

`*.googleapis.com` is one host with two upstream ports:

```yaml
match:
  - headers:
      content-type:
        prefix: application/grpc
route:
  - destination: { host: jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local, port: { number: 8081 } }
# default route -> port 8080
```

`content-type: application/grpc*` is the discriminator every gRPC client sets,
and it is the only reliable one at L7 (host and path are shared).

**h2c on the upstream hop.** `8081` is the emulator's plaintext HTTP/2 port.
Istio derives the upstream protocol from the destination Service port, so
`jaiscloud-gcp`'s 8081 port must be one of:

- `name: grpc` (what the manifest already ships — Istio's port-name protocol
  selection maps `grpc`/`grpc-*` to GRPC), or
- `appProtocol: http2` (explicit, version-proof).

Without either, Envoy treats 8081 as raw TCP and the route cannot work. The
DestinationRule in `destinationrule.yaml` carries the h2 connection-pool
settings for port **8081 only** (`portLevelSettings`), so nothing it configures
touches REST on 8080; note that a DestinationRule cannot itself *select* a
protocol, so fixing the Service port (or `appProtocol`) is the real knob, and it
belongs to whoever owns `deploy/k8s/jaiscloud-gcp.yaml`.

Routes leave `timeout` **unset**, which Istio documents as *no timeout* (and set
`retries.attempts: 0`): LRO polling, big uploads and gRPC streams must not be cut
short, and a silently retried emulator write is a duplicate write. Do not write
`timeout: 0s` as a way to disable it — the Istio 1.32 CRD validation rejects it
(`must be greater than 1ms`), and the field is unnecessary.

## Cluster DNS

The gateway routes on SNI/Host, but a client still has to resolve
`storage.googleapis.com` to the gateway. Two paths, neither required for the
smoke gate:

1. **In-cluster (primary): CoreDNS rewrite.** Map the four suffixes to
   `istio-ingressgateway.istio-system.svc.cluster.local` in the `kube-system`
   Corefile, so every pod resolves the real names to the gateway ClusterIP.
   The exact fragment, the reviewed-patch procedure and the rollback live in
   [`dns.yaml`](dns.yaml) — that file is documentation, because a ConfigMap
   `apply` replaces `data.Corefile` wholesale and would clobber the cluster's
   resolver. Caveat: the rules hijack the four suffixes for the whole cluster.

2. **Per-workload fallback: `hostAliases`.** Add the concrete hosts to one pod
   spec (example in `dns.yaml`). Wildcards cannot be expressed, so list each
   host; there is no port in `/etc/hosts`, so clients use HTTPS/443.

`scripts/istio-smoke.sh` needs neither: it discovers the gateway address with
`kubectl` and dials it with `curl --connect-to` / `grpcurl -authority`, keeping
the real host in the URL so the emulator's authority routing is exercised
honestly.

## Park, switch and DNS/CA

A front door does not change when a substrate is parked: the Gateway, the
VirtualServices, the certificate and any DNS override all stay exactly as they
were, while the backend behind them is scaled to 0. That is fine for a fast
failure and wrong for a transparent one, so know which override you used:

| override | scope | on `--park <cloud>` | on `--switch <target>` | on `--delete <cloud>` |
|---|---|---|---|---|
| CoreDNS rewrite (`dns.yaml`) | every pod in the cluster | the name still resolves to the gateway; its route has no ready backend, so clients hang (or get a fast 503 with `retries.attempts: 0`) | the newly parked substrate's suffixes go stale and the resumed one's become valid again | the name resolves to a gateway route with no backend: remove that suffix's rewrite |
| Pod `hostAliases` | one workload's `/etc/hosts` | the client pod keeps the gateway ClusterIP; nothing to change until it needs the parked backend | repoint/restart the client at the resumed substrate, or keep the gateway (it routes by host) | same as park: the gateway has no backend |
| pinned `clusterIP` | one Service | the Service keeps its address but loses its endpoints (connection refused) | pin the address of the substrate you intend to run, not both | the Service is gone; the address is free again |

Recommendation: while a substrate is parked, remove its suffix from the CoreDNS
Corefile (one line per suffix in `dns.yaml`) and re-add it on resume; keep the
`hostAliases` and `clusterIP` forms pointed at the gateway/Service of whichever
substrate is running. `deploy/deploy.sh --check --json` reports
`dns.rewrites` / `dns.suffixes` / `dns.stale` and warns per stale suffix, so the
check tells you which rule to drop.

The CA is the opposite case: **park keeps it**. `certs/gen.sh` writes the CA
ConfigMap `jaiscloud-ca-cert` into each substrate namespace for managed
workloads that trust the front door; a resumed substrate, its Dataproc drivers
and Cloud Run pods still need it, so do not delete it with the compute. It only
becomes stale when the issuer is gone: if the TLS secret
`istio-system/jaiscloud-wildcard` (and the `Gateway`) no longer exist, a
remaining copy is a stale CA — re-run `certs/gen.sh` or remove the copy. The
preflight reports `ca.aws` / `ca.gcp` / `ca.issuerPresent` and warns once per
namespace that still has a copy without an issuer.

## Addressing the substrates

The files here build the **external** front door (real cloud hostnames, TLS at
the gateway). They are not how a client inside the cluster reaches the
emulators: those Services are ordinary cluster Services, already routable, and
need no mesh object.

In-cluster Service DNS is the first choice:

| substrate | REST | gRPC |
|---|---|---|
| AWS | `jaiscloud.jaiscloud.svc.cluster.local:4566` | — |
| GCP | `jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8080` | `jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8081` (h2c) |

The short forms `jaiscloud:4566` and `jaiscloud-gcp:8080` / `:8081` work only
from a pod in the *same* namespace; anything else should use the
fully-qualified name.

**If a client library or dialer insists on a literal IP**, use one of two
supported options — neither is in the shipped manifests:

1. A Pod `hostAliases` entry for each concrete host (wildcards are not
   expressible, and there is no port field in `/etc/hosts`). This is the
   per-workload option and changes nothing cluster-wide.
2. Pin the Service's `spec.clusterIP` to an unused address inside the cluster's
   Service CIDR (from the API server's `--service-cluster-ip-range`; a
   `kubectl cluster-info dump | grep -m1 service-cluster-ip-range` works where
   the cluster exposes its args). `clusterIP` is
   **immutable**: changing it means deleting and recreating the Service — never
   the namespace, and the data (PVCs, databases) lives outside it. The address
   must not already be allocated to another Service, or the API server rejects
   the create, and it must stay inside the CIDR, which differs per cluster; that
   is why the manifests do not pin one.

**The emulator-host variables take `host:port`, not just an IP.** The official
`PUBSUB_EMULATOR_HOST`, `FIRESTORE_EMULATOR_HOST` and `STORAGE_EMULATOR_HOST`
accept the Service DNS name (for example
`jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8081`), so the Go and Java
clients need no non-routable address. Do not cargo-cult a `169.254.x.x` value:
it is not required, and it will not route on a cluster.

### Clients with a sidecar

The substrates run **no** sidecar, and do not need one. Istio's auto mTLS sends
plaintext to a destination without a sidecar, so a sidecar-injected client
reaches `jaiscloud:4566` (AWS) and `jaiscloud-gcp:8080` (REST) / `:8081` (gRPC
h2c) with **no** `PeerAuthentication` and **no** `DestinationRule`.

A `PeerAuthentication: STRICT` in the *client's* namespace governs inbound
traffic **to that client's workload**; it does not apply to the client's
outbound connection to the emulator, so it does not block this path.

gRPC clients must speak h2c to `8081`. The GCP Service port is named `grpc`
(`rest`, `grpc`, `hms` in `jaiscloud-gcp.yaml`), so Istio selects HTTP/2 for it;
do not rename that port, and do not rewrite the authority (see "Never rewrite
authority" above).

No `ServiceEntry` is needed: both Services are in the mesh's cluster. A
`ServiceEntry` is only for a destination outside it.

## Standalone-binary constraint

Nothing here is load-bearing for the emulators. No mesh asset touches the
process: with no cluster, no Istio and no Postgres both binaries still serve
plaintext on `:4566` (AWS) and `:8080`/`:8081` (GCP), clients still point at them
with `STORAGE_EMULATOR_HOST` / `AWS_ENDPOINT_URL` / `--endpoint-url`, and the
executor default stays `mock`.

## Troubleshooting

| symptom | cause |
|---|---|
| `curl` returns `421 Misdirected Request` / TLS handshake failure | the host you asked for is not one of the four wildcards, or SNI and `Host` disagree |
| GKE call returns an empty `{"clusters":[]}` or a Managed Kafka error | the authority was rewritten or stripped — remove it from the mesh config |
| gRPC calls hang or return `HTTP 415`/`Unimplemented` | the 8081 Service port is not detected as gRPC (port name / `appProtocol`) |
| `404` from the gateway with `no healthy upstream` | the destination Service/namespace name is wrong; use the fully-qualified name |
| Certificate errors in a client | it does not trust the dev CA — pass `--cacert`/`CA_FILE` the `ca.crt` that `gen.sh` wrote (it prints the path) |
| `connection refused` on 443 | nothing published 443: check `istio-ingressgateway`'s ports; on kind (no LoadBalancer controller) use the NodePort or `SMOKE_PORT_FORWARD=1` |
