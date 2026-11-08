# 04 — multi-environment composition (TF4-4)

One root module, applied once per Terraform workspace (`staging`, then `prod`):
two buckets, a Pub/Sub topic, a versioned Secret Manager secret and an audit
topic in a second project. It is the stack a team runs per environment, and it
is the only gate that proves the composition contract holds per workspace — two
isolated states over one configuration — rather than over one shared state.

## What it proves

- **Workspaces are isolated states.** Both environments are applied from the
  same configuration and every resource name carries `terraform.workspace`; the
  two stacks co-exist with different names and `env` labels. Destroying
  `staging` makes only its resources `404`; every `prod` resource stays readable
  and `prod`'s `terraform plan -detailed-exitcode` is still `0` afterwards.
- **`for_each` and `count` instantiate the environment.** The bucket pair is a
  `for_each` over `{input, output}` and the secret's two versions are a
  `count = 2`; both are asserted through the API (two buckets, two versions with
  both payloads), not just through Terraform state.
- **A provider alias routes to its own project.** The audit topic is declared
  with `provider = google.shared` and no explicit `project`. The provider
  resolves the project from the alias (the Terraform-side output names it), and
  the server-side read-back proves the resource actually landed there: its
  response `name` is `projects/<project>-shared/topics/...`, while the same name
  in the default project is `404`. An aliased provider inherits nothing from the
  default one, so `main.tf` gives it its own `pubsub_custom_endpoint` from the
  shared `local.endpoints` map.

## How it runs

```bash
make test-gcp-terraform-pattern PATTERN=04-multienv            # Terraform
make test-gcp-terraform-pattern PATTERN=04-multienv TF_BIN=tofu # OpenTofu
```

The driver (`run.sh`) follows the shared contract in `../lib.sh` plus a
per-workspace phase:

```text
init -> workspace staging -> apply -> read-back asserts -> plan == 0
     -> workspace prod    -> apply -> read-back asserts -> plan == 0
     -> destroy staging   -> staging 404s, prod still readable, prod plan == 0
     -> destroy prod      -> post-destroy 404 + no-orphan list asserts
```

`patterns/lib.sh` gained `workspace_select` / `destroy_workspace` for this
pattern: each workspace holds its own state, and the `EXIT` cleanup destroys
every workspace the pattern selected (best effort — a destroy that fails is
reported, not retried), so a failure mid-run does not leave an environment
behind silently. Patterns that use one state (00–03) are unchanged.

## Boundaries (recorded, not worked around)

- **The second project is not created.** The emulator is project-scoped and
  serves resources for a project id without a `google_project` row, which is the
  same assumption the other patterns make with `var.project`. `00-bootstrap` is
  the pattern that proves project creation, billing and Service Usage; this one
  proves that an alias routes to the project it names. `<project>-shared` must
  still be a valid project id for real GCP (6–30 characters, starts with a
  letter) — the default `test-project` satisfies that; a long `TF_PROJECT` would
  not, which is an environment constraint, not an emulator gap.
- **Secret payloads are read through `versions.access`.** `SecretVersion.get`
  does not carry `payload` — real GCP only populates it on `versions.access`, and
  the emulator matches that. The driver accesses both versions for their
  payloads and asserts that a plain `versions.get` carries no `payload`.
- **Version numbering is not ordered against `count.index`.** The two
  `google_secret_manager_secret_version` resources are independent (both depend
  on the secret, neither on the other), so Terraform may create them in either
  order and the API assigns numbers as the requests arrive. The driver asserts
  that both revisions exist across the pair, not that version 2 holds
  `revision=2`; asserting the latter would be a flake, not a fidelity check.
- **Workspace names are fixed** (`staging`, `prod`) because they live inside the
  driver's throwaway work dir, not in the emulator. Resource names carry a
  per-run random suffix instead; that is what keeps repeated aggregate runs
  against one emulator from colliding.
