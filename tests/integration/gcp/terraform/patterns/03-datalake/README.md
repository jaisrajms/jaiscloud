# Composition pattern 03 — data lake + GKE/Dataproc (TF4-3)

The canonical analytics stack, provisioned as one Terraform graph:

```text
input/output/staging buckets  <-- the Spark job reads and writes here
        |                              |
        |                    Dataproc job (google_dataproc_job)
        |                              |
        +--> BigQuery dataset/table/routine/row-access-policy
                                       |
   GKE cluster --(target)--> Dataproc-on-GKE virtual cluster --(catalog)-->
                              Dataproc Metastore service --> federation
```

`run.sh` follows the shared composition contract (`patterns/lib.sh`):
`init -> apply -> REST read-back asserts -> plan -detailed-exitcode == 0 ->
job/virtual-cluster destroy (pools survive) -> full destroy -> post-destroy
asserts`.

```bash
make test-gcp-terraform-patterns STRICT=1                 # all patterns, Terraform
make test-gcp-terraform-patterns TF_BIN=tofu STRICT=1     # all patterns, OpenTofu
make test-gcp-terraform-pattern PATTERN=03-datalake       # this one, own emulator
```

It is a full apply/destroy run, so it is nightly-only and not part of
`gcp-ci-preflight`. `STRICT=1` makes a SKIP (a missing binary) a failure.

## What it proves

- one apply creates the stack with cross-resource references
  (`bucket.name`, `container_cluster.name`, `metastore_service.id`) and the
  whole graph re-plans clean — the acceptance is the clean second plan, not the
  apply exit code;
- the virtual cluster's two node-pool targets (an explicit 1–3 autoscaled
  controller pool and a named-only driver/executor pool that takes the API's
  1/10 defaults) are readable through `container.v1`, and the virtual-cluster
  read omits the input-only `nodePoolConfig`;
- the provider submits a `google_dataproc_job` pyspark job against the virtual
  cluster, waits for completion, and the job record reads back `DONE` with the
  input/output URIs it was given;
- the Metastore service is `ACTIVE` with its Hive version, the federation names
  it as backend rank `0`, and the database-level IAM policy carries the job
  service account's `roles/metastore.admin` grant;
- destroying the job and then the virtual cluster leaves both reconciled node
  pools readable — real Dataproc does not own a virtual cluster's pools, only
  the GKE cluster delete reclaims them — and the full destroy leaves no orphan
  cluster, bucket, dataset, Metastore service or federation.

## Boundaries (recorded, not worked around)

- **The Spark data plane is not here.** The emulator's Dataproc executor is a
  state machine; nothing is scheduled onto the reconciled node pools and the
  output bucket stays empty. The k3d / Helm / Java half of the customer story —
  a real driver running `spark/wordcount.py`, reading the input object and
  writing the word count — lives in
  [`demo/dpgke-lifecycle`](../../../../../../demo/dpgke-lifecycle/README.md)
  and is exercised by the k3d gate there, not duplicated here.
- **BigQuery does not load the object.** There is no BigQuery load-job surface
  in the emulator, so the table is created with its schema and the input object
  is uploaded separately; the composition proves the graph, not an ELT run.
  External tables are likewise outside the module cases' measured surface.
- **`google_bigquery_table` has `deletion_protection = true` by default** in
  provider 7.x (a client-side destroy guard). The pattern sets it `false`, as
  any composition that tears its own stack down must; it is not an emulator
  gap.
- **Metastore federation `backend_metastores` is a map keyed by rank** in the
  Discovery document (`additionalProperties: BackendMetastore`), and
  `BackendMetastore` has no rank member. The provider's `rank` HCL attribute
  becomes the map key, so a read-back carries `"backendMetastores": {"0": …}`
  and no `"rank"`. The pattern asserts that shape.
- **The GKE control plane is metadata-only** and the Metastore catalog data
  plane is the emulator's shared Hive plane; see the suite README's recorded
  boundaries.
- **GKE is routed by host.** The provider keeps only scheme+host of a custom
  endpoint, so the endpoint must be a `container.*` host
  (`container.localhost` for the default endpoint). `run.sh` fails loudly when
  that name does not resolve.
