# shellcheck shell=bash
#
# Case: terraform-google-cloud-dns
#
# The pinned consumer is terraform-google-modules/terraform-google-cloud-dns
# (module v7.2.1). Its `google_dns_managed_zone` default type is `private`, and
# it emits `private_visibility_config` (VPC networks and GKE clusters) for the
# private/forwarding/peering types, `forwarding_config` for forwarding zones,
# `peering_config` for peering zones and `dnssec_config` for public zones.
# Before TGM4 the emulator's ManagedZone stored none of them, so GET omitted
# them and every configured zone permadiffed: the module's next plan wanted to
# add the block back, and a changed network list or DNSSEC state was silently
# dropped by the PATCH handler.
#
# A second defect surfaced with the gate: the synthesized numeric managed-zone
# id was an unmasked FNV-64, so about half of all zone names produced a value
# above MaxInt64 and the provider's read failed outright ("managed_zone_id: ''
# expected type 'int', got unconvertible type 'string'"). Real Cloud DNS ids are
# small, and the provider reads managed_zone_id as a signed int, so the id is now
# masked to the positive int64 range.
#
# The gate proves the lifecycle converges for all four zone shapes:
#
#   private    (two networks + a GKE cluster + labels) -> assert GET -> No changes
#   mutate     (description/labels changed, third network added) -> apply ->
#              No changes
#   forwarding (two targets, one with forwarding_path) -> assert -> No changes
#   peering    (target network) -> assert -> No changes
#   public     (dnssec off + query logging) -> assert -> No changes
#   mutate     (dnssec on, labels changed) -> apply -> No changes
#
# Every apply log must be free of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-cloud-dns"
MODULE_COMMIT="35cd6de8e647"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-cloud-dns"

ZONE_PRIVATE="jc-module-private"
ZONE_FORWARDING="jc-module-forwarding"
ZONE_PEERING="jc-module-peering"
ZONE_PUBLIC="jc-module-public"

NET_A="projects/$PROJECT/global/networks/default"
NET_B="projects/$PROJECT/global/networks/jc-net-b"
NET_C="projects/$PROJECT/global/networks/jc-net-c"
PEER_NETWORK="projects/$PROJECT/global/networks/jc-peer"
GKE_CLUSTER="projects/$PROJECT/locations/us-central1/clusters/jc-gke"

# write_module generates the root module for one gate phase:
#   private-create | private-mutate | forwarding | peering | public-off | public-on
write_module() {
  local phase="$1"
  local type name domain description labels networks clusters targets peering dnssec logging

  case "$phase" in
    private-create)
      type="private"
      name="$ZONE_PRIVATE"
      domain="private.example.com."
      description="jaiscloud private gate"
      labels='{
    env   = "tf-module"
    owner = "jaiscloud"
  }'
      networks="[\"$NET_A\", \"$NET_B\"]"
      clusters="[\"$GKE_CLUSTER\"]"
      targets="[]"
      peering=""
      dnssec="{}"
      logging="false"
      ;;
    private-mutate)
      type="private"
      name="$ZONE_PRIVATE"
      domain="private.example.com."
      description="jaiscloud private gate v2"
      labels='{
    env = "tf-module-updated"
  }'
      networks="[\"$NET_A\", \"$NET_B\", \"$NET_C\"]"
      clusters="[\"$GKE_CLUSTER\"]"
      targets="[]"
      peering=""
      dnssec="{}"
      logging="false"
      ;;
    forwarding)
      type="forwarding"
      name="$ZONE_FORWARDING"
      domain="forwarding.example.com."
      description="jaiscloud forwarding gate"
      labels='{
    env = "tf-module"
  }'
      networks="[\"$NET_A\"]"
      clusters="[]"
      targets='[
    { ipv4_address = "10.128.0.10", forwarding_path = "private" },
    { ipv4_address = "10.128.0.11" },
  ]'
      peering=""
      dnssec="{}"
      logging="false"
      ;;
    peering)
      type="peering"
      name="$ZONE_PEERING"
      domain="peering.example.com."
      description="jaiscloud peering gate"
      labels='{
    env = "tf-module"
  }'
      networks="[\"$NET_A\"]"
      clusters="[]"
      targets="[]"
      peering="$PEER_NETWORK"
      dnssec="{}"
      logging="false"
      ;;
    public-off)
      type="public"
      name="$ZONE_PUBLIC"
      domain="public.example.com."
      description="jaiscloud public gate"
      labels='{
    env = "tf-module"
  }'
      networks="[]"
      clusters="[]"
      targets="[]"
      peering=""
      dnssec='{
    state         = "off"
    non_existence = "nsec3"
  }'
      logging="true"
      ;;
    public-on)
      type="public"
      name="$ZONE_PUBLIC"
      domain="public.example.com."
      description="jaiscloud public gate v2"
      labels='{
    env = "tf-module-updated"
  }'
      networks="[]"
      clusters="[]"
      targets="[]"
      peering=""
      dnssec='{
    state         = "on"
    non_existence = "nsec3"
  }'
      logging="true"
      ;;
    *)
      echo "ERROR: unknown phase '$phase'" >&2
      exit 2
      ;;
  esac

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "= 7.46.2"
    }
  }
}

variable "endpoint" {
  type    = string
  default = "$ENDPOINT"
}

variable "project" {
  type    = string
  default = "$PROJECT"
}

provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false
  dns_custom_endpoint   = "\${var.endpoint}/dns/v1/"
}

# The module declares the google-beta provider for its reverse_lookup and
# service_directory variants; those stay count = 0 here, but the provider block
# must exist and resolve, so it points at the same emulator.
provider "google-beta" {
  project = var.project
  region  = "us-central1"

  user_project_override = false
  dns_custom_endpoint   = "\${var.endpoint}/dns/v1/"
}

module "dns" {
  source = "$MODULE_DIR"

  project_id  = var.project
  name        = "$name"
  domain      = "$domain"
  type        = "$type"
  description = "$description"

  labels = $labels

  private_visibility_config_networks = $networks
  gke_clusters_list                  = $clusters

  target_name_server_addresses = $targets
  target_network               = "$peering"

  dnssec_config  = $dnssec
  enable_logging = $logging
}
EOF
}

# zone_body fetches a managed zone through the emulator's REST API.
zone_body() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/dns/v1/projects/$PROJECT/managedZones/$1" 2>/dev/null || true
}

# check_zone asserts that a zone's GET body carries each expected substring.
check_zone() {
  local name="$1" zone="$2"
  shift 2
  local body want
  body="$(zone_body "$zone")"
  if [[ -z "$body" ]]; then
    fail "$name (no response for $zone)"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

# check_zone_absent is the negative check: a block the client did not send must
# not be rendered.
check_zone_absent() {
  local name="$1" zone="$2"
  shift 2
  local body want
  body="$(zone_body "$zone")"
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      fail "$name (unexpected: $want)"
      return 0
    fi
  done
  pass "$name"
}

case_main() {
  write_module private-create
  init_phase
  plan_phase "plan (private zone with two networks, a GKE cluster and labels)"
  apply_phase "apply (create private zone)"
  assert_no_api_errors

  echo "== spot checks (private)"
  # `hashicorp/google` expands a partial network name into the full v1 self-link
  # ("https://compute.googleapis.com/compute/v1/projects/.../global/networks/x");
  # the emulator echoes the accepted URL, so match the resource path inside it.
  check_zone "private visibility, networks and GKE clusters read back" "$ZONE_PRIVATE" \
    '"visibility":"private"' \
    '"privateVisibilityConfig":{' \
    "\"networkUrl\":\"https://compute.googleapis.com" \
    "$NET_A\"" "$NET_B\"" \
    "\"gkeClusterName\":\"$GKE_CLUSTER\""
  check_zone "labels read back" "$ZONE_PRIVATE" \
    '"env":"tf-module"' '"owner":"jaiscloud"'
  check_zone_absent "no forwarding/peering/dnssec block was synthesized" "$ZONE_PRIVATE" \
    '"forwardingConfig":{' '"peeringConfig":{' '"dnssecConfig":{'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  write_module private-mutate
  apply_phase "apply (description/labels changed, third network added)"
  assert_no_api_errors
  echo "== spot checks (private mutate)"
  check_zone "mutated description, labels and network list read back" "$ZONE_PRIVATE" \
    '"description":"jaiscloud private gate v2"' \
    '"env":"tf-module-updated"' \
    "$NET_C\"" \
    "$NET_A\""
  plan_no_changes "re-plan after mutation reports no changes"

  write_module forwarding
  apply_phase "apply (forwarding zone with two targets)"
  assert_no_api_errors
  echo "== spot checks (forwarding)"
  check_zone "forwarding targets read back" "$ZONE_FORWARDING" \
    '"visibility":"private"' \
    '"forwardingConfig":{' \
    '"targetNameServers":[' \
    '"ipv4Address":"10.128.0.10"' '"forwardingPath":"private"' \
    '"ipv4Address":"10.128.0.11"'
  check_zone "forwarding zone keeps its private visibility networks" "$ZONE_FORWARDING" \
    "$NET_A\""
  plan_no_changes "forwarding re-plan reports no changes"

  write_module peering
  apply_phase "apply (peering zone)"
  assert_no_api_errors
  echo "== spot checks (peering)"
  check_zone "peering target network reads back" "$ZONE_PEERING" \
    '"visibility":"private"' \
    '"peeringConfig":{' \
    '"targetNetwork":{' \
    "$PEER_NETWORK\""
  check_zone_absent "a peering zone renders no forwarding block" "$ZONE_PEERING" \
    '"forwardingConfig":{'
  plan_no_changes "peering re-plan reports no changes"

  write_module public-off
  apply_phase "apply (public zone with dnssec off and query logging)"
  assert_no_api_errors
  echo "== spot checks (public, dnssec off)"
  check_zone "dnssec config and key specs read back" "$ZONE_PUBLIC" \
    '"visibility":"public"' \
    '"dnssecConfig":{' \
    '"state":"off"' '"nonExistence":"nsec3"' \
    '"defaultKeySpecs":[' \
    '"keyType":"keySigning"' '"keyLength":2048' \
    '"keyType":"zoneSigning"' '"keyLength":1024'
  check_zone "query logging reads back" "$ZONE_PUBLIC" \
    '"cloudLoggingConfig":{' '"enableLogging":true'
  check_zone_absent "a public zone renders no private visibility block" "$ZONE_PUBLIC" \
    '"privateVisibilityConfig":{'
  check_zone_absent "a public zone renders no forwarding/peering block" "$ZONE_PUBLIC" \
    '"forwardingConfig":{' '"peeringConfig":{'
  plan_no_changes "public re-plan reports no changes"

  write_module public-on
  apply_phase "apply (dnssec on, description/labels changed)"
  assert_no_api_errors
  echo "== spot checks (public, dnssec on)"
  check_zone "dnssec state flipped on and key specs survived the PATCH" "$ZONE_PUBLIC" \
    '"state":"on"' '"keyType":"keySigning"' '"keyLength":2048'
  check_zone "labels changed" "$ZONE_PUBLIC" \
    '"env":"tf-module-updated"' '"description":"jaiscloud public gate v2"'
  plan_no_changes "re-plan after dnssec change reports no changes"

  destroy_phase
}
