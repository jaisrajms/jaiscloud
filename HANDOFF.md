# JaisCloud UI — handoff

Context for picking up further UI work. Current as of branch `feat/aws-console-ui-main`
(tip `a8ce3798`).

## Repo & branch

- Working dir: `/home/aamerik/code/jaiscloud-ui`.
- `origin` = https://github.com/aamerik/jaiscloud.git (fork); `upstream` = https://github.com/jaisrajms/jaiscloud.git.
- Current branch: `feat/aws-console-ui-main`. The AWS/UI work lives here.
- Open PR: https://github.com/jaisrajms/jaiscloud/pull/309 — `aamerik:feat/aws-console-ui-main` -> `jaisrajms:develop`.
  Base is **develop**, not main. `develop` already contains the earlier UI work (#307/#308 merged);
  this PR is the newer stack.
- The earlier tooling PR (#307) and UI PR (#308) merged into `raj/ui` -> `develop`. `main` does not have the UI yet.

## Toolchain (not on the default PATH)

- Go: `export PATH=/tmp/opencode/go/bin:$PATH`
- Node/pnpm: `export PATH=/tmp/opencode/node22/bin:$PATH` (Node 22; pnpm via corepack).
  A Node tarball was extracted to `/tmp/opencode/node22` if it needs reinstalling.

## Build / test / run

- Frontend typecheck: `cd ui && pnpm exec tsc -b`
- Lint: `pnpm lint` (must stay error-free; a few known warnings are fine)
- Build assets: `pnpm build` (outputs to `../internal/ui/dist`)
- Binary with UI embedded: `go build -tags ui -o /tmp/opencode/jc-console ./cmd/jaiscloud-aws/`
- GCP binary with UI embedded: `go build -tags ui -o /tmp/opencode/jc-gcp ./cmd/jaiscloud-gcp/`
- Run: `/tmp/opencode/jc-console start --ephemeral --ui --port 4599 --ui-port 4598 --log-level warn`
  - UI: http://localhost:4598/ui/ (LAN: http://10.0.100.115:4598/ui/), gateway: 4599
- Go tests: `go test ./internal/...`; shared UI core tests in `internal/ui/`, AWS UI API tests in `internal/aws/ui/`.
- Always rebuild the binary after UI changes (assets are embedded via `-tags ui`).

## Architecture & conventions (match these)

- React 19 + Vite + TypeScript, **AWS Cloudscape Design System** (`@cloudscape-design/components`).
  One Theme + AWS token overrides in `ui/src/theme/aws/theme.ts`; top-nav colour override in `theme/aws/console.css`.
- Shell: `ui/src/components/Layout.tsx` — TopNavigation (region, account, refresh, live, appearance, Admin),
  SideNavigation "All services" (category groups + "Find services"), BreadcrumbGroup, AppLayout,
  Tools HelpPanel, Flashbar notifications.
- Service menu is **server-driven**: `GET /api/ui/v1/services` returns `ServiceDescriptor[]`
  (`id,label,category,rootPath,children,tier,note`). Definitions live in `internal/ui/types.go`; each cloud
  contributes its catalog via a `coreui.Registrar` — AWS in `internal/aws/ui/services.go`, GCP in `internal/gcp/ui/`.
  Frontend: `hooks/useServices.ts`, grouped in `components/nav.ts`.
- Lists: `components/ResourceTable.tsx` — PropertyFilter, pagination, CollectionPreferences, optional selection,
  sticky header, auto-sorting on filterable columns, `actions` slot. Page = `ContentLayout` + `Header` + `ResourceTable`.
- Detail pages: `ContentLayout` + `Header` + `Tabs` + `Container` + `KeyValuePairs`/`Table`.
- Dialogs: Cloudscape `Modal` + `Form`/`Input`/`Select`/`Textarea`.
- Cross-cutting helpers:
  - `components/notifications.tsx` — `useNotifications()` -> Flashbar
  - `lib/status.ts` — `resourceStatus()` -> `StatusIndicator`
  - `lib/date.ts` — `formatDate()`
  - `lib/timeRange.ts` — `rangeToWindow()` / `RELATIVE_OPTIONS` for DateRangePicker
  - `components/JsonEditor.tsx` — lazy Cloudscape CodeEditor (ace)
  - `components/ServiceTierBadge.tsx` — Popover explaining metadata-only/preview
  - `components/ResourceDetailsModal.tsx` — shared read-only "View details" dialog
    (`{ visible, onDismiss, header, items: {label,value}[], columns? }`)
- No Tailwind (never installed). Avoid raw `<table>/<button>/<input>`; use Cloudscape.
  Intentional exceptions: dark log viewers (`<pre>`) in Lambda test / Log stream view.

## Done

- Whole app converted to Cloudscape; service list/detail pages consistent; Admin panel redesigned;
  Admin link in top nav.
- Tables with filter/pagination/column prefs/selection/Actions/sorting/sticky across all services.
- Notifications, StatusIndicators, CopyToClipboard, charts (CloudWatch metrics + dashboard widgets),
  CodeEditor (DynamoDB item, Lambda test, dashboards, IAM/secret bodies), S3 upload, SQS tag editing
  (AttributeEditor), Glue catalog TreeView, DateRangePicker + log-group Autosuggest, tier badges.
- Backend changes in this PR:
  - `/api/ui/v1/services` descriptors + tier
  - SQS tag writes (TagQueue/UntagQueue) + GetTags type fix + CreateQueue tag persistence
  - CloudWatch `GetMetricStatistics` comma-split fix
  - S3 object upload exposed to the UI
  - UI list handlers coerce typed provider slices via `uihelper.AsSlice` (fixes empty lists —
    RDS/EKS/Glue/etc. — after pagination preserves the element type; commit `84112a54`)
  - KMS key mapping reads `CreationDate` (int64 Unix) so Created renders
  - SFN `ListStateMachines` enriches each machine via `DescribeStateMachine`
    (roleArn/status/definition) and maps `creationDate` as int64/float64
- The SplitPanel "inspector" was added then **removed** (duplicated detail pages). Keep the Modal approach.
- **"View details" modals** (`ResourceDetailsModal`) on every list-only service: EC2, RDS, EKS,
  CloudWatch alarms, KMS, CloudFormation stacks, ELBv2, EventBridge buses, Step Functions state
  machines, ElastiCache, Kinesis, SES, Glue crawlers, Firehose. EC2/RDS/EKS were migrated onto the
  shared component. Alarms/KMS expose it as the first row-actions item; the rest use a trailing
  inline-link column. Services with a real detail route (EMR, EMR on EKS, DynamoDB, Lambda, Logs,
  S3, SNS, SQS, Secrets Manager) keep their routes and were not given modals.

## Open work

1. ~~Emulator bug: RDS/EKS list endpoints return empty after a successful create.~~
   **Fixed** in `84112a54` (`uihelper.AsSlice`).
2. ~~"View details" only on EC2/RDS/EKS.~~ **Done** — see the Done section.
3. **GCP UI (next)**: non-AWS the catalog returns empty. Placeholder pages: `ui/src/services/gcp/index.tsx`,
   `azure/index.tsx`. When GCP lands, add GCP descriptors behind `cfg.Cloud == "gcp"` (per-cloud descriptor builder
   behind `/api/ui/v1/services`).
4. Possible next upgrades: `Wizard` create flows (Tiles/Steps) for EC2/RDS/ECS; expandable/tree rows for S3
   prefixes; Popover help on more fields.
5. Packaging already covers the UI: `Dockerfile` builds+embeds with `-tags ui`; goreleaser `-tags=ui`;
   release workflow builds assets; CI has a `ui` job. SPA caching: hashed assets immutable, index.html no-cache.

## Working style

- Keep changes consistent with existing patterns; run `tsc`/`lint`/`build` (and `go build`/`test`) before committing.
- Commit to `feat/aws-console-ui-main` and push — that updates PR #309 (base `develop`).
- Keep replies concise.
