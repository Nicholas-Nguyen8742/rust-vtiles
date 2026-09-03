# Tenant Isolation (Sequence 5)

End-to-end tenant isolation for the CRE vector tile platform. CRE tenants
hold confidential portfolio, parcel, valuation, lease, flood, zoning, and ESG
data; isolation protects client confidentiality, supports SOC2-aligned
controls, and is the foundation for multi-tenant growth.

**Zero-trust model:** tenant identity comes from the *authenticated
principal*, never from client-supplied request bodies. Every resource is
tenant-scoped. Every access path is authorized, logged, and tested. Tile
delivery never relies on security-by-obscurity of tenant ids in URLs.

## Control map

| Layer | Control | Implementation | Verification |
|---|---|---|---|
| API | Tenant-aware authorization | Bearer token + `X-Tenant-Id` claim header; `require_auth` middleware validates the claim pattern on every tenant-scoped route (`/api/v1/*`, `/tiles/*`) | `tests/api_tenant_isolation.rs` |
| Jobs | Ownership checks | `JobRecord.tenantId`; `check_tenant_access` gate on status/replay/content endpoints | cross-tenant job tests |
| Storage | Tenant-scoped paths | `staging/{tenantId}/`, `tiles/{tenantId}/`, `quarantine/{tenantId}/`, `manifests/{tenantId}/`; TLS-only bucket policies (`terraform/s3.tf`) | path tests + Terraform |
| Tiles | Private delivery | CloudFront OAC (no public S3); API ownership gate; tenant prefix in cache key; signed URLs/cookies for confidential layers | tile access tests |
| Encryption | KMS | SSE-KMS pipeline key; per-tenant keys/aliases for high-sensitivity tenants (`terraform/kms.tf`) | security review |
| DLQ/Replay | Tenant-scoped recovery | Replay validates `caller == job.tenantId`; DLQ records carry tenant | replay authorization tests |
| Events | Tenant propagation | Every `PipelineEvent` carries validated `tenantId`; worker alignment check | event contract test |
| Logs/Audit | Tenant-scoped observability | Structured stage logs with `tenantId`; decision audit trail; restricted dashboards | audit tests + alerts |

## Tenant identity model (TI-01/TI-02)

Tenant ids must match `^[a-z0-9-_]{3,64}$`
(`vtile_pipeline::tenant::is_valid_tenant_id`). Rejected: empty ids,
uppercase, whitespace, separators (`/`, `\`), and traversal (`../`). Resource
ids (layerId/jobId/fileName) additionally reject separators and traversal
segments (`is_valid_resource_id`).

`TenantContext` propagates `tenantId` + optional `userId` + `servicePrincipal`
+ `roles` through jobs, workers, and events. In production this resolves from
OIDC/JWT claims (`sub`, `tenantId`, `roles`); locally the static token binds
the `X-Tenant-Id` claim header.

**Error policy:** `401` for missing/invalid auth or tenant claim; `403` for
authenticated-but-unauthorized access; `404` for non-owned layers/rollback
(existence-hiding reduces tenant enumeration). See
[`ERRORS.md`](ERRORS.md#tenant-isolation-errors-sequence-5).

## Worker-side alignment (TI-02)

`run_job` validates before any storage access:

1. `is_valid_tenant_id(job.tenantId)` — else `TENANT_MISMATCH`.
2. `is_valid_resource_id(job.layerId)` — else `TENANT_MISMATCH`.
3. `tenant_alignment_holds(tenantId, sourceUri)` — the source URI must carry
   the job's own tenant prefix; a URI carrying another tenant's prefix is
   refused rather than read.

Every emitted event serializes the validated `tenantId` (contract test).
Replay never changes tenant identity, and cross-tenant replay is refused.

## Private tile delivery (TI-03)

- Buckets are private: Block Public Access + TLS-only bucket policies;
  the tile bucket is reachable only through CloudFront OAC.
- The CloudFront cache key includes the full tenant path, so cache entries
  cannot cross tenants.
- For confidential layers, mint **signed URLs (5–15 min TTL)** or **signed
  cookies** (session-aligned) for the requesting tenant only; a
  Lambda@Edge viewer-request hook is the alternative when the token model
  supports edge validation (`terraform/cloudfront.tf`).

## DLQ, replay, and log isolation (TI-04)

- DLQ records carry `tenantId`; replay validates `caller == job.tenantId`
  and denies cross-tenant attempts (`403`), audited under the caller.
- Quarantine is tenant-prefixed; incident responders see only their tenant
  unless escalated via break-glass.
- Stage logs and the decision audit trail carry `tenantId`; tenant-scoped
  audit queries never expose another tenant's records.

### Break-glass access

Privileged cross-tenant access requires: an elevated role, a ticket/incident
reference, and always emits an audit event. Locally this is audit-only —
send `X-Break-Glass-Ref: <ticket>` on an ops request; the event is recorded
as `tenant.break_glass` and counted (`privileged_access_count`) but grants no
extra privilege. Production grants time-bound elevated IAM roles.

## Monitoring and compliance (TI-06)

Decision auditing records every ALLOW/DENY on control-plane operations
(`tenant.access.decision`) plus cross-tenant denials
(`tenant.access.denied`), scoped to the *caller* tenant. Metrics:
`tenant_authorization_denied_total`, `cross_tenant_access_attempt_total`,
`authorization_failure_count`, `privileged_access_count`,
`replay_operation_total` (served at `GET /internal/metrics`).

Alert thresholds (Terraform `cloudwatch.tf`):

- **P2** `CrossTenantDenialSpike` — more than 5 cross-tenant denials.
- **P2** `PrivilegedAccessOccurred` — any break-glass event.
- (Plus the Sequence 4 pipeline/tile alerts.)

Compliance reporting is tenant-scoped via `GET /api/v1/ops/audit`; audit
records are append-only and retain tenant context for SOC2 investigation.

## Automated cross-tenant negative tests (TI-05)

Two synthetic tenants (`tenant-alpha`, `tenant-beta`) run the authorization
matrix in CI. CI fails on any cross-tenant access success.

| Access attempt | Expected |
|---|---|
| Tenant A reads A layer | `200` |
| Tenant A reads B layer | `404` (existence-hiding) |
| Layer listing (A) | only A's layers |
| Tenant A reads B job status | `403` |
| Tenant B accesses A tile path | `403` |
| Tenant B replays A job | `403` |
| Tenant B feeds A job content | `403` |
| Upload body tenant ≠ caller | `403` |
| Missing tenant claim (auth on) | `401 MISSING_TENANT_CLAIM` |
| Malformed/traversal claim | `401 INVALID_TENANT_CLAIM` |
| Invalid body tenantId / fileName | `400 INVALID_TENANT_ID` / `INVALID_RESOURCE_ID` |
| Traversal layerId in tile path | `400`/`404` (never resolves) |
| Worker source URI with foreign tenant | `TENANT_MISMATCH` |
| Every event carries tenant | contract assertion |
| Replay preserves tenant | contract assertion |
| Audit query scoping | pinned to caller tenant |

Run: `make isolation-tests` (or `cargo test -p vtile-api --test
api_tenant_isolation` + `cargo test -p vtile-pipeline --test
tenant_isolation`).

## Local → production mapping

| Local | Production |
|---|---|
| Static bearer token + `X-Tenant-Id` header | OIDC/JWT tenant claim via Cognito + API Gateway authorizer |
| Tenant-prefixed local paths | S3 tenant prefixes + IAM prefix policies |
| API ownership gates | API Gateway authorizer + Lambda checks |
| CloudFront OAC + API gate | OAC + signed URLs/cookies or Lambda@Edge auth |
| `X-Break-Glass-Ref` audit hook | Time-bound elevated IAM role + incident ref |
| Append-only `audit.jsonl` | CloudTrail + access logs → data lake (SOC2 retention) |
