#!/usr/bin/env bash
#
# certs/gen.sh — dev CA + wildcard TLS material for the jaiscloud front door.
#
# The gateway terminates TLS for the REAL cloud hostnames, so one certificate
# must cover every host either substrate answers on:
#
#   *.amazonaws.com        AWS data + control plane (S3 virtual-hosted style,
#                          STS, SQS, DynamoDB, Lambda, execute-api, ...)
#   *.googleapis.com       GCP REST (:8080) and gRPC (:8081)
#   *.run.app              Cloud Run data plane
#   *.cloudfunctions.net   Cloud Functions trigger plane
#
# X.509 wildcards match exactly ONE label, so the deep names the emulators
# synthesize need their own SANs:
#
#   {id}-{token}.{location}.run.app                (internal/gcp/service/run)
#   {api}.execute-api.{region}.amazonaws.com       (internal/aws execute-api)
#   s3.{region}.amazonaws.com                      (every regional AWS endpoint)
#   {bucket}.s3.{region}.amazonaws.com             (S3 virtual-hosted style)
#   api.ecr.{region}.amazonaws.com                 (two-label service hosts)
#   streams.dynamodb.{region}.amazonaws.com
#
# Those are generated per location/region from RUN_APP_LOCATIONS (default:
# "us-central1"), AWS_REGIONS (default: "us-east-1"), AWS_SERVICE_PREFIXES
# (default: "api.ecr streams.dynamodb s3-control s3-object-lambda") and
# EXTRA_SANS appends anything else (space separated, DNS: values only).
#
# Why AWS needs two levels beyond `*.amazonaws.com`: X.509 wildcards match
# exactly one label, so `*.amazonaws.com` covers `sts.amazonaws.com` but NOT
# `s3.us-east-1.amazonaws.com` (three labels) and not
# `bucket.s3.us-east-1.amazonaws.com` (four). Every AWS SDK defaults to a
# regional endpoint, so without `*.{region}.amazonaws.com` and
# `*.s3.{region}.amazonaws.com` the front door would fail client-side
# certificate validation for S3, DynamoDB, SQS, … even though the gateway routes
# them correctly (Gateway host matching is a suffix match, unlike X.509).
#
# Artifacts (private keys stay OUT of the repo — OUT_DIR defaults to a cache dir
# under $HOME, never the working tree):
#
#   ca.crt  ca.key        the dev CA (ca.crt is what clients must trust)
#   tls.crt tls.key       the leaf, SANs above
#
# Cluster objects created (idempotent, apply-based):
#
#   Secret    jaiscloud-wildcard   in istio-system   (kubernetes.io/tls,
#                                   tls.crt/tls.key + ca.crt for clients)
#   ConfigMap jaiscloud-ca-cert    in each of CM_NAMESPACES (default
#                                   "jaiscloud jaiscloud-gcp"), key `ca.crt`,
#                                   labelled jaiscloud.io/issuer=mesh — the
#                                   preflight only judges labelled copies as
#                                   JaisCloud's own (an unlabelled copy is a
#                                   host-issued CA it does not diagnose)
#
# The ConfigMap shape is not invented here: internal/platform/config.go defaults
# JAISCLOUD_PLATFORM_TLS_CA_SOURCES to
#
#   []CASource{{Name: "jaiscloud",
#               Source: CASourceRef{Kind: "configMap",
#                                   Name: "jaiscloud-ca-cert",
#                                   Key:  "ca.crt"}}}
#
# i.e. a ConfigMap named jaiscloud-ca-cert with one key, ca.crt, holding PEM.
#
# Usage:
#   deploy/k8s/istio/certs/gen.sh                  # generate if absent, then apply
#   deploy/k8s/istio/certs/gen.sh --force          # regenerate (new CA!), then apply
#   deploy/k8s/istio/certs/gen.sh --generate-only  # files only, no kubectl
#
# Env:
#   OUT_DIR             where the CA + leaf land (default
#                       ${XDG_CACHE_HOME:-$HOME/.cache}/jaiscloud/istio-ca)
#   KUBE_CONTEXT        kubectl --context value (default: current context)
#   SECRET_NS           namespace of the TLS secret (default istio-system)
#   SECRET_NAME         TLS secret name (default jaiscloud-wildcard)
#   CM_NAME             CA ConfigMap name (default jaiscloud-ca-cert)
#   CM_NAMESPACES       spaces-separated namespaces for CM_NAME (default
#                       "$NS_AWS $NS_GCP", i.e. "jaiscloud jaiscloud-gcp" for the
#                       shipped layout; a custom install only has to set NS_AWS /
#                       NS_GCP, or CM_NAMESPACES="$NS_AWS $NS_GCP")
#   DAYS                certificate lifetime in days (default 825)
#   RUN_APP_LOCATIONS   locations for the deep *.run.app SANs (default us-central1)
#   EXECUTE_API_REGIONS regions for the deep execute-api SANs (default us-east-1)
#   AWS_REGIONS         regions for the AWS depth SANs (default us-east-1)
#   AWS_SERVICE_PREFIXES two-label AWS service hosts that need a SAN one level
#                       deeper than `*.{region}.amazonaws.com` (default
#                       "api.ecr streams.dynamodb s3-control s3-object-lambda")
#   EXTRA_SANS          extra DNS SANs, space separated (default empty)
#
# --force semantics: everything under OUT_DIR is regenerated, so the CA identity
# changes. The Secret and ConfigMaps are then overwritten (kubectl apply), which
# means every client that trusted the old ca.crt must re-import the new one and
# running pods must be restarted to pick up the new ConfigMap contents. Without
# --force an existing CA is reused untouched and the apply step is a no-op —
# that is what makes re-running this script safe.
set -euo pipefail

PROG="$(basename "$0")"

FORCE=0
GENERATE_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --force)          FORCE=1 ;;
    --generate-only)  GENERATE_ONLY=1 ;;
    --help|-h)
      # The header comment is the documentation; print it without the shebang.
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0
      ;;
    *)
      echo "$PROG: unknown argument: $arg (try --help)" >&2
      exit 2
      ;;
  esac
done

OUT_DIR="${OUT_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/jaiscloud/istio-ca}"
SECRET_NS="${SECRET_NS:-istio-system}"
SECRET_NAME="${SECRET_NAME:-jaiscloud-wildcard}"
CM_NAME="${CM_NAME:-jaiscloud-ca-cert}"
# The CA ConfigMap belongs in each substrate namespace. Follow a custom layout
# when NS_AWS/NS_GCP are set (the same variables deploy/deploy.sh reads), and
# keep the shipped names otherwise.
CM_NAMESPACES="${CM_NAMESPACES:-${NS_AWS:-jaiscloud} ${NS_GCP:-jaiscloud-gcp}}"
DAYS="${DAYS:-825}"
RUN_APP_LOCATIONS="${RUN_APP_LOCATIONS:-us-central1}"
EXECUTE_API_REGIONS="${EXECUTE_API_REGIONS:-us-east-1}"
AWS_REGIONS="${AWS_REGIONS:-us-east-1}"
AWS_SERVICE_PREFIXES="${AWS_SERVICE_PREFIXES:-api.ecr streams.dynamodb s3-control s3-object-lambda}"
EXTRA_SANS="${EXTRA_SANS:-}"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf '%s: error: %s\n' "$PROG" "$*" >&2; exit 1; }

command -v openssl >/dev/null 2>&1 || die "openssl is required"

# ── SAN list ────────────────────────────────────────────────────────────────
SANS=(
  "*.amazonaws.com"
  "*.googleapis.com"
  "*.run.app"
  "*.cloudfunctions.net"
)
for loc in $RUN_APP_LOCATIONS; do
  SANS+=("*.${loc}.run.app")
done
for region in $EXECUTE_API_REGIONS; do
  SANS+=("*.execute-api.${region}.amazonaws.com")
done
# Regional endpoint of every AWS service (s3/dynamodb/sqs/…) and the
# virtual-hosted S3 form, which is one label deeper. The global S3 form is
# covered by `*.s3.amazonaws.com`.
SANS+=("*.s3.amazonaws.com")
for region in $AWS_REGIONS; do
  SANS+=("*.${region}.amazonaws.com")
  SANS+=("*.s3.${region}.amazonaws.com")
  # Two-label service hosts (api.ecr, streams.dynamodb, …) need their own SAN:
  # `*.{region}.amazonaws.com` only covers one label before the region.
  for prefix in $AWS_SERVICE_PREFIXES; do
    SANS+=("*.${prefix}.${region}.amazonaws.com")
  done
done
for extra in $EXTRA_SANS; do
  SANS+=("$extra")
done

san_extfile() {
  local out="$1"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=serverAuth,clientAuth"
    printf 'subjectAltName=%s\n' "$(printf 'DNS:%s,' "${SANS[@]}" | sed 's/,$//')"
  } > "$out"
}

# ── generate ────────────────────────────────────────────────────────────────
mkdir -p "$OUT_DIR"
chmod 700 "$OUT_DIR"

if [ "$FORCE" -eq 1 ]; then
  log "generating a NEW dev CA in $OUT_DIR (--force): clients must re-trust ca.crt"
  rm -f "$OUT_DIR"/ca.crt "$OUT_DIR"/ca.key "$OUT_DIR"/tls.crt "$OUT_DIR"/tls.key \
        "$OUT_DIR"/tls.csr "$OUT_DIR"/ca.srl "$OUT_DIR"/leaf.ext
fi

if [ -s "$OUT_DIR/ca.crt" ] && [ -s "$OUT_DIR/ca.key" ] && \
   [ -s "$OUT_DIR/tls.crt" ] && [ -s "$OUT_DIR/tls.key" ]; then
  log "reusing existing CA + leaf in $OUT_DIR (use --force to regenerate)"
else
  log "generating dev CA in $OUT_DIR"
  # CA: 10 years is fine for a throwaway dev trust anchor; the leaf below is
  # short enough to be harmless if it leaks.
  openssl req -x509 -newkey rsa:4096 -sha256 -nodes \
    -days 3650 \
    -keyout "$OUT_DIR/ca.key" -out "$OUT_DIR/ca.crt" \
    -subj "/O=jaiscloud/CN=jaiscloud-dev-ca" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >/dev/null 2>&1

  log "generating wildcard leaf for ${#SANS[@]} SANs"
  san_extfile "$OUT_DIR/leaf.ext"
  openssl genrsa -out "$OUT_DIR/tls.key" 2048 >/dev/null 2>&1
  openssl req -new -sha256 \
    -key "$OUT_DIR/tls.key" -out "$OUT_DIR/tls.csr" \
    -subj "/O=jaiscloud/CN=*.googleapis.com" \
    >/dev/null 2>&1
  openssl x509 -req -sha256 \
    -in "$OUT_DIR/tls.csr" \
    -CA "$OUT_DIR/ca.crt" -CAkey "$OUT_DIR/ca.key" -CAcreateserial \
    -days "$DAYS" \
    -extfile "$OUT_DIR/leaf.ext" \
    -out "$OUT_DIR/tls.crt" \
    >/dev/null 2>&1
  rm -f "$OUT_DIR/tls.csr"
fi

chmod 600 "$OUT_DIR/ca.key" "$OUT_DIR/tls.key"

# Verify the pair before it ever reaches the cluster. The host checks cover one
# name at every depth the front door claims (a single-label check would not catch
# a missing regional SAN).
openssl verify -CAfile "$OUT_DIR/ca.crt" "$OUT_DIR/tls.crt" >/dev/null 2>&1 \
  || die "leaf does not verify against $OUT_DIR/ca.crt"
for host in \
  storage.googleapis.com \
  sts.amazonaws.com \
  s3.us-east-1.amazonaws.com \
  bucket.s3.us-east-1.amazonaws.com \
  dynamodb.us-east-1.amazonaws.com \
  api.ecr.us-east-1.amazonaws.com \
  streams.dynamodb.us-east-1.amazonaws.com \
  us-central1-my-project.cloudfunctions.net \
  svc-abc.us-central1.run.app \
  abc.execute-api.us-east-1.amazonaws.com; do
  openssl x509 -in "$OUT_DIR/tls.crt" -noout -checkhost "$host" >/dev/null \
    || die "leaf SANs do not cover $host (widen AWS_REGIONS/RUN_APP_LOCATIONS or EXTRA_SANS)"
done

log "SANs on the leaf:"
openssl x509 -in "$OUT_DIR/tls.crt" -noout -ext subjectAltName 2>/dev/null | sed -n '2,$p' | tr ',' '\n' | sed 's/^ */    /' >&2

if [ "$GENERATE_ONLY" -eq 1 ]; then
  log "--generate-only: wrote $OUT_DIR/{ca.crt,ca.key,tls.crt,tls.key}; not touching the cluster"
  log "clients trust: $OUT_DIR/ca.crt"
  exit 0
fi

# ── apply ───────────────────────────────────────────────────────────────────
command -v kubectl >/dev/null 2>&1 || die "kubectl is required (or use --generate-only)"

if [ -n "${KUBE_CONTEXT:-}" ]; then
  kc() { kubectl --context "$KUBE_CONTEXT" "$@"; }
else
  kc() { kubectl "$@"; }
fi

# Refuse to invent namespaces: the CA ConfigMap belongs to a substrate namespace
# that the deploy path owns, and creating one here would produce a half-built
# substrate.
for ns in "$SECRET_NS" $CM_NAMESPACES; do
  kc get namespace "$ns" >/dev/null 2>&1 \
    || die "namespace $ns does not exist — deploy the namespaces first (or use --generate-only)"
done

log "applying Secret $SECRET_NS/$SECRET_NAME (tls.crt, tls.key, ca.crt)"
kc -n "$SECRET_NS" create secret generic "$SECRET_NAME" \
    --type=kubernetes.io/tls \
    --from-file=tls.crt="$OUT_DIR/tls.crt" \
    --from-file=tls.key="$OUT_DIR/tls.key" \
    --from-file=ca.crt="$OUT_DIR/ca.crt" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null

for ns in $CM_NAMESPACES; do
  log "applying ConfigMap $ns/$CM_NAME (key ca.crt, label jaiscloud.io/issuer=mesh)"
  kc -n "$ns" create configmap "$CM_NAME" \
      --from-file=ca.crt="$OUT_DIR/ca.crt" \
      --labels=jaiscloud.io/issuer=mesh \
      --dry-run=client -o yaml | kc apply -f - >/dev/null
done

cat >&2 <<EOF

==> done
    CA (what clients trust):  $OUT_DIR/ca.crt
    leaf (served by gateway): $OUT_DIR/tls.crt
    secret:                   $SECRET_NS/$SECRET_NAME
    CA ConfigMaps:            $CM_NAMESPACES (key ca.crt)

    Managed workloads pick the CA up through JAISCLOUD_PLATFORM_TLS_CA_SOURCES
    (default: configMap/jaiscloud-ca-cert key ca.crt). They read it when a
    workload is CREATED, so restart them after a --force regeneration.

    For a client on this machine:
      export SSL_CERT_FILE=$OUT_DIR/ca.crt          # Python/google clients
      curl --cacert $OUT_DIR/ca.crt --resolve storage.googleapis.com:443:\$(kubectl -n $SECRET_NS get svc istio-ingressgateway -o jsonpath='{.spec.clusterIP}') https://storage.googleapis.com/storage/v1/b?project=p
EOF
