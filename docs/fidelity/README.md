# Fidelity inventories

Committed, machine-generated definitions the GCP gates assert against. They are
regenerated/checked by `make gen-gcp-field-coverage` / `check-gcp-field-coverage`
and `make test-gcp-provider-capture`.

| file | source | gate |
|---|---|---|
| `field-coverage.csv` | gRPC descriptors + vendored Discovery documents (`tools/fieldgen`) | `check-gcp-field-coverage` |
| `reference-coverage.csv` | `google.api.resource_reference` annotations + documented references | `check-gcp-field-coverage` |
| `provider-attributes.csv` | `terraform providers schema -json`, classified per attribute | `make test-gcp-provider-capture` + every module gate |
| `terraform-provider-schema.json` | a compacted provider-schema snapshot of the mapped resource types | the weekly drift workflow |
| `terraform-scorecard.md` | `make test-gcp-terraform-all` output, pinned tool/provider versions | the nightly (`terraform-nightly.yml`); reviewed per Terraform session |

## provider-attributes.csv

One row per attribute of a provider resource type the module gates exercise.
`classification` says how the attribute relates to the emulator's API surface:

- `served` — the provider sends it and the emulator serves it; `api_field` names
  the `field-coverage.csv` field it joins to (a sent attribute whose catalog row
  is missing or `unsupported` fails the gate).
- `no-catalog` — served, but the resource is outside the FC1 catalog scope.
- `client-side` — the provider never sends it (the TGM15 class); the reason must
  say why.
- `output-only` — computed, never part of a request body.
- `unsupported` — a client can send it but the emulator does not serve it; a
  capture that sends it fails the gate.
- `not-exercised` — the pinned module does not configure it. If a capture starts
  sending it, the gate fails until it is reclassified.

Regenerate the row skeleton after a provider bump with:

```bash
terraform providers schema -json > /tmp/schema.json
go run ./tools/tfcapture -init-mapping -schema /tmp/schema.json \
  -mapping docs/fidelity/provider-attributes.csv -resources google_storage_bucket,...
```

The classifications are hand-maintained: only the attribute list is generated.
The mapping is written against the `hashicorp/google` provider schema; when a
module also pulls `hashicorp/google-beta` in, the loader keeps google's resource
definition (beta is its superset mirror and its extra attributes are out of the
mapping's scope). The weekly drift workflow regenerates
`terraform-provider-schema.json` from the same resource list, so a new provider
attribute shows up as a diff before it can reach a capture run.
