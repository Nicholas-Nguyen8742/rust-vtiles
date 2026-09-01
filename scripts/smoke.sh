#!/bin/sh
# End-to-end smoke test against a running local API.
#
# Drives the full happy path (upload -> normalize -> tile -> serve) plus the
# negative contracts (404 unknown layer, 422 zoom out of range), and prints a
# PASS/FAIL summary. Exits non-zero on any failure so it can gate CI.
#
# The covered-tile check computes the z/x/y from the job's published bounding
# box rather than hardcoding coordinates, so it keeps working if the fixture
# geometry moves.
#
# Usage:
#   make smoke                      # uses http://127.0.0.1:8080
#   API_BASE=http://host:port make smoke
#
# Requires: API running (make run-local) and fixtures present (make fixtures).

set -u

API_BASE="${API_BASE:-http://127.0.0.1:8080}"
TENANT="${TENANT:-tenant-acme}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$ROOT/tests/fixtures/simple-parcels.geojson"

PASS=0
FAIL=0

say_pass() { PASS=$((PASS + 1)); printf '  PASS  %s\n' "$1"; }
say_fail() { FAIL=$((FAIL + 1)); printf '  FAIL  %s\n' "$1"; }

if ! command -v python3 >/dev/null 2>&1 || ! command -v curl >/dev/null 2>&1; then
    echo "error: curl and python3 are required" >&2
    exit 1
fi
if [ ! -f "$FIXTURE" ]; then
    echo "error: fixture missing: $FIXTURE (run: make fixtures)" >&2
    exit 1
fi

json_field() {
    printf '%s' "$1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$2',''))"
}

# tile_for '<bbox-json-array>' <zoom>  -> prints "x y"
tile_for() {
    printf '%s' "$1" | python3 -c "
import sys, json, math
b = json.load(sys.stdin)
z = int('$2')
lon = (b[0] + b[2]) / 2.0
lat = (b[1] + b[3]) / 2.0
n = 2 ** z
x = int((lon + 180.0) / 360.0 * n)
r = math.radians(lat)
y = int((1.0 - math.log(math.tan(r) + 1.0 / math.cos(r)) / math.pi) / 2.0 * n)
print(x, y)
"
}

echo "== healthz =="
if curl -fsS "$API_BASE/healthz" >/dev/null 2>&1; then
    say_pass "GET /healthz"
else
    say_fail "GET /healthz (is the API running? make run-local)"
    echo; echo "smoke: $PASS passed, $FAIL failed"; exit 1
fi

echo "== happy path: upload -> process -> serve =="
LAYER="smoke-parcels-$$"

resp="$(curl -fsS -X POST "$API_BASE/api/v1/ingest/uploads" \
    -H 'Content-Type: application/json' \
    -d "{\"tenantId\":\"$TENANT\",\"layerId\":\"$LAYER\",\"fileName\":\"smoke.geojson\",\"contentType\":\"application/geo+json\",\"sourceFormat\":\"GEOJSON\",\"metadata\":{\"name\":\"Smoke Parcels\",\"category\":\"PARCEL\",\"tags\":[\"smoke\"]}}")" || resp=""
JOB="$(json_field "$resp" jobId)"
if [ -n "$JOB" ]; then say_pass "POST /ingest/uploads -> $JOB"; else say_fail "POST /ingest/uploads: $resp"; fi

STATUS=""
if [ -n "$JOB" ]; then
    code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
        "$API_BASE/api/v1/ingest/uploads/$JOB/content" \
        -H 'Content-Type: application/geo+json' --data-binary "@$FIXTURE")"
    if [ "$code" = "202" ]; then say_pass "PUT content (HTTP 202)"; else say_fail "PUT content (HTTP $code)"; fi

    # Poll the job until it reaches a terminal state (COMPLETED/FAILED).
    i=0
    while [ "$i" -lt 30 ]; do
        job_json="$(curl -fsS "$API_BASE/api/v1/jobs/$JOB" 2>/dev/null)" || job_json=""
        STATUS="$(json_field "$job_json" status)"
        case "$STATUS" in
            COMPLETED | FAILED) break ;;
        esac
        i=$((i + 1))
        sleep 1
    done
    if [ "$STATUS" = "COMPLETED" ]; then
        say_pass "job reached COMPLETED"
    else
        say_fail "job terminal status was '$STATUS' (wanted COMPLETED)"
    fi
fi

# Fetch one tile the published bbox is known to cover (zoom 14, PARCEL range).
if [ "$STATUS" = "COMPLETED" ]; then
    bbox="$(printf '%s' "$job_json" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin).get('boundingBox') or []))")"
    xy="$(tile_for "$bbox" 14)"
    X="${xy% *}"; Y="${xy#* }"
    code="$(curl -s -o /dev/null -w '%{http_code}' \
        "$API_BASE/tiles/$TENANT/$LAYER/14/$X/$Y.pbf")"
    if [ "$code" = "200" ]; then
        say_pass "GET covered tile 14/$X/$Y (HTTP 200)"
    else
        say_fail "GET covered tile 14/$X/$Y (HTTP $code, wanted 200)"
    fi

    # Sequence 2 US-AP-04: the same tile through the explicit-version URL.
    VERSION="$(python3 -c "import json; print(json.load(open('$DATA_DIR/manifests/$TENANT/$LAYER/latest.json'))['tileVersion'])" 2>/dev/null || echo "")"
    if [ -n "$VERSION" ]; then
        code="$(curl -s -o /dev/null -w '%{http_code}' \
            "$API_BASE/tiles/$TENANT/$LAYER/versions/$VERSION/14/$X/$Y.pbf")"
        if [ "$code" = "200" ]; then
            say_pass "GET versioned tile $VERSION 14/$X/$Y (HTTP 200)"
        else
            say_fail "GET versioned tile $VERSION 14/$X/$Y (HTTP $code, wanted 200)"
        fi
    else
        say_fail "latest.json missing or unreadable (no tileVersion)"
    fi

    # Zoom below the published PARCEL range (min 10) -> 422.
    code="$(curl -s -o /dev/null -w '%{http_code}' \
        "$API_BASE/tiles/$TENANT/$LAYER/3/2/3.pbf")"
    if [ "$code" = "422" ]; then
        say_pass "GET out-of-range zoom (HTTP 422)"
    else
        say_fail "GET out-of-range zoom (HTTP $code, wanted 422)"
    fi
fi

echo "== negative contracts =="
# Unknown layer -> 404.
code="$(curl -s -o /dev/null -w '%{http_code}' \
    "$API_BASE/tiles/$TENANT/no-such-layer/14/0/0.pbf")"
if [ "$code" = "404" ]; then
    say_pass "GET unknown layer (HTTP 404)"
else
    say_fail "GET unknown layer (HTTP $code, wanted 404)"
fi

echo
echo "smoke: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
