# Error Taxonomy

Every pipeline failure carries a **stable, machine-readable code** and — for
job-level failures — the **workflow stage** where it stopped. The same code
appears in all four failure surfaces, so log alerts, API clients, and replay
tooling can key off one vocabulary:

| Surface | Field |
|---|---|
| API error responses (TRD §8) | `{"error": {"code", "message"}}` |
| Job status `GET /api/v1/jobs/{jobId}` | `errorCode`, `failedStage` |
| `vector.tile.job.failed` events (TRD §9) | `errorCode`, `errorMessage` |
| Quarantine report (`data/quarantine/{tenant}/{job}/error-report.json`) | `errorCode`, `failedStage`, `errorMessage` |

Codes are defined in `vtile_ingest::IngestError::error_code` and
`vtile_pipeline::job::error_classification` — this document is their
normative reference.

## Failed-stage values

`failedStage` is one of the TRD §10 workflow states (job-level failures only):
`VALIDATING`, `NORMALIZING`, `TILING`, `PUBLISHING`. Upload-gate rejections
fail before a job runs, so they have no stage.

## Ingest errors (source-data problems)

These are the failures where the uploaded bytes themselves are the problem.
They are the **only** failures that quarantine the source for replay
(`data/quarantine/{tenantId}/{jobId}/`).

| Code | HTTP | Typical stage | Meaning | Remediation |
|---|---:|---|---|---|
| `INVALID_FILE_TYPE` | 422 | — (upload gate, no job) | File extension or `contentType` does not match the declared `sourceFormat` | Fix `fileName`/`contentType` (`.geojson`/`.json` for GEOJSON, `.zip` for SHAPEFILE) |
| `MISSING_SHAPEFILE_COMPONENTS` | 422 | `NORMALIZING` | Zip bundle is missing `.shp`, `.shx`, or `.dbf` | Re-export the bundle with all mandatory members |
| `INVALID_SHAPEFILE` | 422 | `NORMALIZING` | Corrupt/unreadable SHP/DBF/SHX content, or not a valid zip archive | Re-export; verify the bundle opens in a GIS tool |
| `INVALID_GEOJSON` | 422 | `NORMALIZING` | Malformed JSON or unsupported GeoJSON structure (e.g. GeometryCollection) | Validate against RFC 7946; split collections into typed features |
| `EMPTY_DATASET` | 422 | `NORMALIZING` | Zero features in the source (or zero-byte upload body at the content endpoint) | Provide a non-empty dataset |
| `GEOMETRY_ERRORS` | 422 | `NORMALIZING` | All features failed geometry repair (degenerate rings, non-finite coordinates); partial failures are warnings, not errors | Fix the flagged geometries; re-upload |
| `UNKNOWN_CRS` | 422 | `NORMALIZING` | Shapefile has no `.prj`; TRD §10 requires explicit user confirmation | Re-upload with `.prj`, or replay with `--assume-wgs84` / `"assumeCrsWgs84": true` |
| `UNSUPPORTED_CRS` | 422 | `NORMALIZING` | CRS detected but no safe reprojection path to EPSG:4326 | Reproject the source to WGS84 before upload |
| `ENCODING_ERROR` | 422 | `NORMALIZING` | Attribute encoding not decodable (UTF-8 preferred, ISO-8859-1 fallback) | Re-export DBF as UTF-8 |
| `PAYLOAD_TOO_LARGE` | 413 | — / `NORMALIZING` | Upload bytes exceed the limit (default 2 GiB; enforced at the content endpoint and re-checked pre-parse) | Split the dataset; raise `--max-upload-bytes` only by policy |
| `FILE_TOO_LARGE` | 413 | `NORMALIZING` | Feature count exceeds the 1,000,000 cap (TRD §14 scalability) | Split into layers by region/category |
| `INGEST_FAILED` | 422 | `NORMALIZING` | Catch-all for other source problems | Inspect the quarantine report message |
| `INTERNAL_ERROR` | 500 | any | Unexpected I/O failure during ingest | Retry; check disk/permissions if persistent |

Notes:

- `GEOMETRY_ERRORS` vs `EMPTY_DATASET`: a source with *no* features at all is
  `EMPTY_DATASET`; a source whose features are all unrecoverable is
  `GEOMETRY_ERRORS`. Individual bad features among good ones produce warnings
  and still publish (TRD §4 repair-first policy).
- The 413 codes also fire at the HTTP layer before any job exists: the content
  endpoint rejects oversized bodies directly (`PAYLOAD_TOO_LARGE`) and empty
  bodies as `EMPTY_DATASET` (422).

## Infrastructure errors (500 — retryable, not quarantined)

Tile/store failures are infrastructural; replaying the same bytes without
fixing the environment adds nothing, so these are **not** quarantined.
Atomic-publishing codes (`PUBLISH_*`, `PROMOTION_CONFLICT`, `ROLLBACK_FAILED`)
are detailed in [`PUBLISHING.md`](PUBLISHING.md).

| Code | HTTP | Typical stage | Meaning |
|---|---:|---|---|
| `TILE_SIZE_EXCEEDED` | 500 | `TILING` | A tile exceeded the size limit. Reserved: the size-mitigation ladder (drop attributes → coalesce → simplify → split) always converges before this fires today; the variant exists for stricter future policies |
| `TILE_GENERATION_FAILED` | 500 | `TILING` | MVT preparation/encoding failure (e.g. no tileable features after preparation) |
| `STORE_ERROR` | 500 | any | Job/layer persistence failure (local: disk; production: DynamoDB) |
| `PUBLISH_VALIDATION_FAILED` | 500 | `PUBLISHING` | Candidate version failed the completeness gate (tile count, zero-byte, zoom coverage, aggregate checksum). The previous published version stays live; fix the source and re-publish (Sequence 2 US-AP-02) |
| `PROMOTION_CONFLICT` | 500 | `PUBLISHING` | Conditional promotion lost — the layer's authoritative pointer moved underneath this publisher. Retry with a fresh candidate (Sequence 2 US-AP-03) |
| `ROLLBACK_FAILED` | 500 | — | Rollback rejected: unknown layer, missing/corrupt target version, or no publication yet. Via the ops API this surfaces as `422 ROLLBACK_INVALID_TARGET` (Sequence 2 US-AP-05) |
| `REPLAY_NOT_ALLOWED` | 500 (API: 422) | — | Replay refused (Sequence 3 US-03/US-04): the failure class is permanent (`PERMANENT_VALIDATION` other than `UNKNOWN_CRS`), the manual-replay limit (3) is exhausted, or the job is cancelled. Fix the source and submit a new upload — see `docs/RECOVERY.md` |
| `PIPELINE_ERROR` | 500 | any | Other orchestration failure (unsupported format reaching the runner, missing bbox, replay preconditions, ...) |

## Upload-gate and serving responses (API layer)

These never reach the pipeline; they enforce the TRD §8/§13 contracts.

| Code | HTTP | Where | Meaning |
|---|---:|---|---|
| `INVALID_REQUEST` | 400 | `POST /ingest/uploads` | Missing `tenantId` or `layerId` |
| `UNSUPPORTED_FORMAT` | 422 | `POST /ingest/uploads` | `sourceFormat` outside MVP (GeoJSON/Shapefile only; KML/GeoPackage/FlatGeobuf are post-MVP) |
| `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH` | 409 | `POST /ingest/uploads` | `Idempotency-Key` reused with a different payload (Sequence 1 US-02) — fix the payload or mint a new key |
| `ROLLBACK_INVALID_TARGET` | 422 | `POST /ops/layers/{id}/rollback` | Rollback target missing/corrupt, or the layer was never published (Sequence 2 US-AP-05). Missing `targetTileVersion`/`reason` → `400 INVALID_REQUEST` |
| `JOB_ALREADY_ACTIVE` | 409 | `POST /ops/jobs/{id}/replay` | Replay attempted while the job is still being processed (Sequence 3 US-04) |
| `REPLAY_NOT_ALLOWED` | 422 | `POST /ops/jobs/{id}/replay` | Permanent failure class, replay limit exhausted, or cancelled job (Sequence 3 US-03/US-04); the response carries remediation guidance |
| `UNAUTHORIZED` | 401 | any (auth enabled) | Missing/invalid bearer token |
| `FORBIDDEN` | 403 | jobs, tiles | Token tenant does not match the resource tenant (TRD §13 isolation) |
| `JOB_NOT_FOUND` | 404 | `GET /jobs/{id}`, content PUT | Unknown job id; content PUTs for unknown jobs are also recorded under `data/orphans/` (Sequence 1 US-03) |
| `LAYER_NOT_FOUND` | 404 | layers, tiles | Unknown layer, or another tenant's layer (existence is not leaked) |
| `LAYER_NOT_PUBLISHED` | 404 | tiles | Layer exists but has no manifest (never published) |
| `ZOOM_OUT_OF_RANGE` | 422 | tiles | `z` outside the layer's published range or beyond the tile grid (TRD §8.5) |
| `INVALID_TILE_COORDINATES` | 400 / 422 | tiles | Non-numeric `z/x/y`, or `x`/`y` outside the zoom's grid |

The tile endpoint additionally returns **`204 No Content` for empty tiles** —
not an error, per TRD §8.5/US-03, so map clients do not treat voids as
failures.

Content PUTs for jobs that are **already active or terminal** return a
**`202` acknowledgment** and increment `duplicateEventCount` instead of
starting new work (Sequence 1 US-03 duplicate-event suppression). The
`vector.tile.job.replay_requested` audit event (replay operations) is
documented in [`IDEMPOTENCY.md`](IDEMPOTENCY.md).

## Idempotency and concurrency errors (Sequence 1)

These surface through `job.failed` classification as `PIPELINE_ERROR` (HTTP
500) at the API boundary, but are distinct in the pipeline and tests; full
semantics in [`IDEMPOTENCY.md`](IDEMPOTENCY.md).

| Error | Meaning | Operator action |
|---|---|---|
| `JobAlreadyExists` | Conditional create lost — the `jobId` is already registered | Converge on the existing record via the idempotency key (automatic in the API) |
| `StateConflict` | Conditional transition rejected: stale expected status or illegal edge | Inspect `stateVersion` history; usually a redelivery race — retry |
| `LeaseConflict` | Job owned by another worker's active lease | Back off; the owning worker finishes or the lease expires (900 s) |
| `JobAlreadyActive` | Replay attempted while the job is still processing (`JOB_ALREADY_ACTIVE`) | Wait for the terminal state, then replay if still needed |

## Quarantine and replay

Layout (written atomically; only ingest failures):

```text
data/quarantine/{tenantId}/{jobId}/
  input.bin          # exact uploaded bytes
  error-report.json  # { jobId, tenantId, layerId, sourceFormat, sourceUri,
                     #   requestedZoomRange, errorCode, errorMessage,
                     #   failedStage, quarantinedAt }
```

Replay (`vtile replay` / `make replay-job`) semantics — Sequence 1 US-05
hardening:

1. Replay rules by stored status: `FAILED` → allowed (canonical DLQ
   redrive); `COMPLETED` → allowed only with `--create-new-version`;
   active jobs → rejected with `JobAlreadyActive` (`JOB_ALREADY_ACTIVE`);
   `CANCELLED` → rejected.
2. Tenant must match; replays never cross tenant boundaries.
3. Source bytes resolve from the quarantine first; when absent
   (completed-job re-publish) they fall back to the staged upload.
4. Every replay emits `vector.tile.job.replay_requested` (`requestedBy`,
   `reason`, `createNewVersion`) and persists a `ReplayAudit` on the job.
5. The job is reset to `QUEUED` with `error`/`errorCode`/`failedStage` and
   any stale lease cleared, then re-runs the full workflow.
6. Each run mints a **fresh `tileVersion`** and swaps `manifest.json` /
   `latest.json` atomically — replay either publishes a complete new version
   or leaves the previous one untouched (TRD §14: "DLQ replay must not
   duplicate published tiles").
7. For `UNKNOWN_CRS` failures, `--assume-wgs84` is the TRD §10 "user
   confirmation" that unlocks the retry.
