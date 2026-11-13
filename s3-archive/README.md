# Workflow Archive Viewer for Temporal Cloud

Temporal Cloud deletes closed workflow executions once they reach the namespace [retention period](https://docs.temporal.io/evaluate/cloud/limits) (1 to 90 days, default 30). The Workflow Archive Viewer keeps them viewable: a developer enters a workflow ID and opens any of its executions in the Temporal event history view, with encrypted payloads decoded.

The source is Temporal Cloud [Workflow History Export](https://docs.temporal.io/cloud/export), which copies closed executions to S3. No Temporal server or database is added.

## 1. Architecture

The converter workflow can be started in two ways: an hourly Temporal Schedule (included) or an S3 ObjectCreated event that invokes a Lambda function (to be added; see [Trigger options](#trigger-options)).

```mermaid
flowchart LR
  subgraph Cloud[Temporal Cloud]
    NS[(Exported namespace)]
    subgraph CNS[Converter namespace]
      SCH["Option 1: Schedule, hourly"] --> WF[ExportConverterWorkflow]
    end
  end
  subgraph AWS[Plymouth AWS]
    EXP[("S3 export/<br/>hourly batch files")]
    L["Option 2: Lambda"]
    W[Converter worker]
    ARC[("S3 archive/<br/>one object per execution")]
    V["Workflow Archive Viewer"]
    CS[Codec server]
  end
  NS -- "History Export" --> EXP
  EXP -. "S3 ObjectCreated" .-> L
  L -. "Start workflow" .-> WF
  WF -. tasks .-> W
  W -- read --> EXP
  W -- write --> ARC
  V -- "read (server-side role)" --> ARC
  Dev((Developer)) -- SSO --> V
  Dev -. "browser: encrypted payloads" .-> CS
```

| Component | What it is | Folder |
|---|---|---|
| Converter | Temporal workflow, activities and Schedule that split each export batch into one S3 object per workflow execution. Based on Temporal's [Parquet export sample](https://github.com/temporalio/samples-python/tree/main/cloud_export_to_parquet). | [`converter/`](converter/) |
| Workflow Archive Viewer | `temporalio/ui` at a pinned commit plus [`s3-archive.patch`](ui/s3-archive.patch). Adds the `/s3-archive` pages and three read-only API routes. Developers need no S3 access; the Workflow Archive Viewer reads S3 with its own role. | [`ui/`](ui/) |
| Codec server | Java [remote data encoder](https://github.com/temporalio/sdk-java/tree/main/temporal-remote-data-encoder) with the samples-java [`CryptCodec`](https://github.com/temporalio/samples-java/tree/main/core/src/main/java/io/temporal/samples/encryptedpayloads). Demo only; replace with your codec (section 6). | [`codec-server/`](codec-server/) |

## 2. Why a converter is needed

| History Export writes | Consequence |
|---|---|
| Hourly batch files, many executions per file, as one protobuf message | One execution cannot be read without downloading and parsing the whole file |
| `temporal-workflow-history/export/<ns>/<yyyy>/<mm>/<dd>/<hh>/00/<uuid>`: batch hour and a random name | Nothing in the key identifies the workflows inside |
| An execution can arrive in any batch up to 24 h after it closes | A date-based lookup would scan a day of extra files |

Temporal Cloud has no Archival API over exported data, and AWS has no service that reshapes protobuf in place. Converting each batch once, when it arrives, turns every lookup into a single S3 prefix list.

| Design choice | Reason |
|---|---|
| One S3 object per execution, keyed by namespace and workflow ID | Lookup without a database or index |
| Execution summary (type, status, start, close) as object metadata | The executions list loads without downloading histories |
| A `_converted/` marker per export file | Progress lives in S3; re-runs skip finished files, a crash re-converts only unfinished ones |
| Payloads copied unchanged | The converter holds no keys and data stays encrypted at rest; decoding happens only when viewed |
| Converter as a Temporal workflow | Retries, execution history and backfill on the platform Plymouth already runs |

## 3. Converter

```mermaid
flowchart TD
  Start(["Schedule, Lambda<br/>or convert_once.py"]) --> H["Files to convert<br/>Schedule: previous 6 UTC export hours<br/>convert_once.py: given hours<br/>Lambda: the new export object"]
  H --> L[["list_unconverted (per hour)"]]
  L --> D1{"_converted/<br/>marker?"}
  D1 -- yes --> Skip([Skip])
  D1 -- no --> C[["convert_file (10 files in parallel)"]]
  C --> D2{"History<br/>readable?"}
  D2 -- yes --> Put["Write archive/&lt;ns&gt;/&lt;workflowId&gt;/&lt;runId&gt;.json<br/>with summary metadata"]
  D2 -- no --> Q["Copy file to _quarantine/<br/>with .errors.txt"]
  Put --> M["Write _converted/ marker"]
  Q --> M
```

| Piece | Behavior | Code |
|---|---|---|
| Schedule | Cron `45 * * * *` (Cloud writes hour folder H at about H+1:15). 1-day catch-up window; a run is skipped while the previous one is running; 50-minute run timeout. | [`schedule.py`](converter/schedule.py) |
| Workflow | Input selects the files: `export_keys` (Lambda) converts those objects only; `hours` (`convert_once.py`) lists those hours; neither (Schedule) lists the 6 hours before `TemporalScheduledStartTime`. Returns hours, files, executions and quarantined files. | [`workflows.py`](converter/workflows.py) |
| Activities | `list_unconverted` (2 min) and `convert_file` (15 min, heartbeat per execution). Retry from 5 s to 5 min, no attempt limit. | [`activities.py`](converter/activities.py) |
| Parsing | No I/O. Run ID is `originalExecutionRunId`, or the new run ID for a reset. Status comes from the last event. | [`archive_format.py`](converter/archive_format.py) |
| Worker | Task queue `s3-archive-converter`, 10 activity threads; serves every namespace that exports to the same bucket | [`worker.py`](converter/worker.py) |

| Failure | Result |
|---|---|
| S3 error (throttling, network, access) | Retried until the run times out. The Schedule revisits recent files on its next run. A file that keeps failing holds up the files queued after it. |
| One unreadable history | Only that execution is skipped; the file goes to `_quarantine/` with an `.errors.txt` |
| Whole file unreadable | The file goes to `_quarantine/` |
| Crash before the marker | The file is converted again; writes overwrite, as do duplicate deliveries from Cloud |
| Worker down for more than 6 hours with the Schedule | Run `convert_once.py` for the missed hours |

### Trigger options

Both options are valid. Choose based on how quickly archives need to appear and which components your team wants to operate. Neither reduces Cloud History Export's delivery delay.

#### Option 1: Temporal Schedule

The included [Schedule](converter/schedule.py) starts the converter at :45 each hour and checks the previous six export hours.

- **Pros:** no Lambda to deploy; revisiting recent hours recovers unprocessed files without relying on event delivery.
- **Cons:** new files wait for the next scheduled run; S3 is listed even when no files arrive. Outages beyond the lookback window need a backfill.

#### Option 2: S3 ObjectCreated event → Lambda → workflow

Configure an [S3 notification to invoke Lambda](https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html) when an export object arrives. Lambda reads the namespace from the object key and calls [`start_workflow`](https://python.temporal.io/temporalio.client.Client.html#start_workflow) on `ExportConverterWorkflow` with `ConvertInput(namespace=..., export_keys=[key])` on task queue `s3-archive-converter`. Lambda returns once Temporal accepts the start; the converter worker converts that one object.

- **Pros:** conversion starts seconds after an object arrives; no S3 listing.
- **Cons:** adds a Lambda deployment, invocation permissions, Temporal credentials and network access from Lambda, and monitoring for failed starts. A missed or failed notification is not retried by anything else; recover with `convert_once.py`.

The workflow already accepts `export_keys`; only the Lambda function and notification are not included. When adding them:

- Filter notifications to `temporal-workflow-history/export/` so archive and marker writes do not trigger new conversions.
- S3 can deliver [duplicate notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html). Derive the workflow ID from the object key, use ID conflict policy `USE_EXISTING` and reuse policy `ALLOW_DUPLICATE_FAILED_ONLY`, and treat "already started" as success.
- Set a workflow execution timeout, Lambda retries and an on-failure destination for starts that fail.

Use the Schedule for simpler periodic collection, or Lambda when prompt conversion after export arrival justifies the extra AWS integration.

## 4. Viewing a workflow execution

```mermaid
sequenceDiagram
  actor Dev as Developer
  participant UI as Workflow Archive Viewer (browser)
  participant US as Workflow Archive Viewer (Go server)
  participant S3
  participant CS as Codec server
  Dev->>UI: /s3-archive · namespace + workflow ID
  UI->>US: GET /api/v1/s3-archive/namespaces/{ns}/workflows/{id}/runs
  US->>S3: list archive/{ns}/{id}/ + object metadata
  US-->>UI: executions, newest first
  Dev->>UI: open an execution
  UI->>US: GET …/runs/{runId}/history
  US->>S3: get archive/{ns}/{id}/{runId}.json
  US-->>UI: events
  UI->>CS: POST /decode (encrypted payloads only)
  CS-->>UI: decoded payloads
```

```text
temporal-workflow-history/export/<ns>/<yyyy>/<mm>/<dd>/<hh>/00/<file>   # written by Cloud; source of record
archive/<ns>/<url-encoded workflowId>/<runId>.json                      # {"events": [...]}, payloads unchanged
archive/_converted/<export key>.done                                    # one marker per converted file
archive/_quarantine/<export key>[.errors.txt]                           # files with unreadable histories
```

`archive/` is `ARCHIVE_PREFIX` in `ARCHIVE_BUCKET` (default: the export bucket). Workflow IDs are URL-encoded into one key segment, so `order-1/shipment` and `order-1` stay separate.

## 5. Set up

**Prerequisites**

| Area | Requirement |
|---|---|
| Temporal Cloud | History Export enabled on the namespace to archive. A separate converter namespace and API key, so the converter's own executions are not exported. |
| AWS, converter | `s3:GetObject` on `temporal-workflow-history/export/<ns>/*`, `s3:PutObject` on `archive/*` |
| AWS, Workflow Archive Viewer | `s3:GetObject` on `archive/*` |
| AWS, both | `s3:ListBucket` limited to those prefixes (`s3:prefix` condition) |
| Local | Docker with Compose |

```bash
cp .env.example .env            # settings below
docker compose up -d --build    # codec server :8888, converter worker, Workflow Archive Viewer :8080

# Convert existing export folders (UTC hours)
docker compose run --rm converter python convert_once.py 2026/11/12/15 2026/11/12/16

# Option 1 only: create the hourly Schedule
docker compose run --rm converter python schedule.py create     # --paused to create it paused
```

For Option 2, configure the Lambda integration described under [Trigger options](#trigger-options) instead of creating the Schedule. Keep the converter worker running with either option.

Open <http://localhost:8080/s3-archive>, choose the namespace, enter a workflow ID and open an execution.

| `.env` setting | Meaning |
|---|---|
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY` | Converter namespace, where the worker and Schedule run |
| `EXPORTED_NAMESPACE`, `EXPORT_BUCKET` | Namespace being archived and its export bucket |
| `ARCHIVE_BUCKET`, `ARCHIVE_PREFIX` | Archive location (defaults: export bucket, `archive`); the Workflow Archive Viewer reads the same values |
| `AWS_REGION` | AWS region for the bucket |
| `AWS_PROFILE` | Local AWS profile. This kit uses a profile from the mounted `~/.aws`; use whichever AWS authentication your organization uses for the converter and the Workflow Archive Viewer. |


## 6. Production changes

| Area | In this kit | Change to |
|---|---|---|
| Codec | Demo `CryptCodec`; its key travels in payload metadata | Your workers' codec (for example KMS): point `TEMPORAL_CODEC_ENDPOINT` in [`docker-compose.yml`](docker-compose.yml) at your codec server, or replace [`CryptCodec.java`](codec-server/src/main/java/codecserver/CryptCodec.java) |
| Codec endpoint | `http://localhost:8888` | A private URL the developer's browser can reach. The server has no authentication and allows any origin. |
| Workflow Archive Viewer login | Off | OIDC through the Workflow Archive Viewer's `TEMPORAL_AUTH_*` settings |
| AWS credentials | Local profile from the mounted `~/.aws` | Your organization's AWS authentication (for example a workload IAM role); both services use the default AWS credential chain |
| Namespaces | One `EXPORTED_NAMESPACE` | Option 1: one Schedule per namespace. Option 2: one notification covering each namespace's export prefix; Lambda reads the namespace from the key. |

## 7. UI patch

[`ui/Dockerfile`](ui/Dockerfile) fetches `temporalio/ui` at [`UPSTREAM_COMMIT`](ui/UPSTREAM_COMMIT), applies the patch and builds. Only the files below change. To upgrade, update `UPSTREAM_COMMIT` and `UI_COMMIT` in the Dockerfile and re-apply the patch.

```mermaid
flowchart LR
  subgraph Web["SvelteKit app (new route group)"]
    L["(s3-archive) layout<br/>loads /settings only"] --> P1[Search page]
    L --> P2["History page<br/>EventSummaryTable + codec decode"]
  end
  subgraph Go["Workflow Archive Viewer server (Go)"]
    R["route/api.go: 3 routes"] --> H["api/s3archive.go"]
  end
  P1 & P2 --> R
  H --> S3[(S3 archive/)]
```

| File | Change |
|---|---|
| `server/server/api/s3archive.go` | New. Read-only routes: list namespaces, list executions, get history. Validates namespace and run ID, encodes the workflow ID as the converter does, applies the Workflow Archive Viewer's auth check (JWT verified when OIDC is on). Configured by `S3_ARCHIVE_BUCKET`, `S3_ARCHIVE_PREFIX`, `S3_ARCHIVE_REGION`. |
| `server/server/route/api.go` | Registers the routes ahead of the catch-all Temporal API route |
| `server/go.mod`, `go.sum` | AWS SDK for Go v2 |
| `src/routes/(s3-archive)/` | New route group whose layout loads only `/settings`, so no Temporal server is needed. The history page reuses `EventSummaryTable`, so codec decoding works as for live workflows, and blocks links to live-workflow pages. |
| `src/lib/services/s3-archive-service.ts` | Fetch helpers for the routes |
