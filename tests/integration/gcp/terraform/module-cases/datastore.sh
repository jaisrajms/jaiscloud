# shellcheck shell=bash
#
# Case: terraform-google-modules/terraform-google-cloud-datastore (TF3-6)
#
# The pinned consumer is the CFT `terraform-google-cloud-datastore` module at
# v2.0.1. It is not a provider-resource module at all: per its own README it
# "supports index creation by shelling out to gcloud". The module
#
#   1. renders the caller's `index.yaml` into `tmp/index.yaml` (`local_file`),
#   2. runs `scripts/create-indexes.sh` from a `null_resource` local-exec, which
#      calls `gcloud datastore indexes cleanup/create`.
#
# So there is no `google_*` resource and no provider-to-emulator request to
# capture. This case is plan-only by construction (wave-plan TF3-6):
#
#   init -> validate -> plan (no apply, no destroy)
#
# and asserts that the module renders both of its resources from the index
# document the case passes in. That measures the module's contract; it is
# deliberately not an apply, because applying would run `gcloud` against a real
# Datastore endpoint (the emulator does not serve the Datastore Admin
# `projects.indexes.*` surface — the fidelity matrix records it under "published
# methods with no registered route"), and this repository never delegates a gate
# to a network/tool step.
#
# Boundaries, recorded rather than worked around:
#
#  * The Datastore Admin index API (`datastore.projects.indexes.create/get/list/
#    delete`) is not served; gcloud would receive a 404. The served Datastore
#    surface is the data API (lookup/runQuery/commit/allocateIds), which the
#    service/transport/conformance suites cover. This is a recorded gap
#    (TF3-6-1), not a Terraform emulator defect: no provider resource type maps
#    to it.
#  * The module has no `versions.tf`; the root module pins the client-side
#    providers (`local`, `null`) explicitly so the case is deterministic.

MODULE_REPO="terraform-google-cloud-datastore"
MODULE_COMMIT="1616a282bbf4"

# Plan-only by construction (wave-plan TF3-6).
PLAN_ONLY=1

# The index document is the module's whole input contract. It mirrors the
# module's own examples/simple_example/yaml/index.yaml.
INDEX_YAML='indexes:
- kind: Task
  ancestor: no
  properties:
  - name: done
  - name: priority
    direction: desc
'

write_module() {
  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "= 2.5.2"
    }
    null = {
      source  = "hashicorp/null"
      version = "= 3.2.4"
    }
  }
}

variable "project" {
  type    = string
  default = "$PROJECT"
}

module "datastore" {
  source = "$MODULE_DIR"

  project = var.project
  indexes = <<-YAML
$INDEX_YAML
  YAML
}
EOF
}

case_main() {
  write_module
  init_phase
  plan_only_phase "plan (module datastore index document)"

  plan_includes "module.datastore.local_file.cloud-datastore-index-file" \
    "plan renders the index document file"
  plan_includes "module.datastore.null_resource.cloud-datastore-indices" \
    "plan renders the gcloud index resource"

  # The module's contract is the rendered index document, so assert the plan
  # carries the caller's kind and property, not merely that it ran.
  if grep -q 'kind: Task' "$WORK/plan.log" &&
    grep -q 'name: priority' "$WORK/plan.log"; then
    pass "plan carries the requested index kind and properties"
  else
    fail "plan does not carry the index document (see $WORK/plan.log)"
  fi

  echo "== plan-only: no apply (the module shells out to gcloud; Datastore index admin is unserved)"
}
