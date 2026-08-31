# Coordinate Precision: Source vs. MVT Quantization

TRD §4 requires source coordinates to retain **at least 7 decimal places**
(~1.1 cm at the equator, ~0.84 cm at NYC latitudes) and forbids aggressive
simplification at high zoom. This document separates the two precision
regimes that requirement touches, because MVT tiles *cannot* represent
7-decimal coordinates at low/medium zooms — that is inherent to the format,
not a bug in the pipeline.

## 1. Two regimes

| Stage | Representation | Precision |
|---|---|---|
| Source → normalization → tiling input | IEEE-754 `f64` lon/lat | full double precision, no rounding anywhere in `vtile-ingest` |
| MVT tile output | integer tile-local units (extent 4096) | quantized by zoom (table below) |

The pipeline never rounds lon/lat before encoding (TRD §14: "No silent
coordinate truncation"). Quantization happens exactly once, at encode time,
inside `TileTransform`, by rounding to the nearest integer extent unit.

## 2. Quantization per zoom

At zoom `z`, one tile spans `360 / 2^z` degrees of longitude. One extent
unit is therefore

```text
deg_per_unit(z) = 360 / 2^z / 4096
meters_per_unit(z, φ) ≈ deg_per_unit(z) × 111319.49 × cos(φ)
```

(Mercator is conformal, so north–south ground resolution matches
east–west at the same latitude.)

At NYC latitude (φ ≈ 40.75°, cos φ ≈ 0.758):

| Zoom | deg/unit | Ground resolution |
|---:|---:|---:|
| 10 | 8.58e-5 | ~7.2 m |
| 12 | 2.15e-5 | ~1.8 m |
| 14 | 5.36e-6 | ~0.45 m |
| 15 | 2.68e-6 | ~0.23 m |
| 16 | 1.34e-6 | ~0.11 m |
| 17 | 6.71e-7 | ~5.7 cm |
| 18 | 3.35e-7 | ~2.8 cm |
| 19 | 1.68e-7 | ~1.4 cm |
| 20 | 8.38e-8 | ~0.7 cm |

**Implication:** 7-decimal (~1 cm) fidelity requires tiles at **zoom ≥ 20**.
The TRD parcel range 10–16 quantizes boundaries to 0.1–7 m, which is
appropriate for *visualization* (sub-meter screen accuracy at typical web
map scales) but not for survey-grade geometry.

## 3. What this means for CRE workflows

- **Visualization / comps overlays (TRD zooms 10–16):** safe. Quantization
  error (≤ ~0.5 m at z14+) is far below screen resolution and below the
  positional accuracy of most assessor parcel sources (~1 m).
- **Boundary-sensitive workflows (valuation disputes, setback/FAR checks):**
  do not rely on tile geometry. Either:
  1. publish an additional high-zoom layer at 18–20 for the area of
     interest (config: `zoom_range` up to 20 — the encoder's max, see
     `TileConfig::validate`), or
  2. query source features (GeoJSON artifact retained in staging) rather
     than tile geometry.
- This directly answers **TRD open question 7** ("Do parcels require
  high-fidelity boundaries at zoom 18?"): only zoom ≥ 18 tiles carry
  sub-3 cm geometry; if any workflow needs that, generate 17–20 for the
  target parcels as a separate layer.

## 4. Guardrails implemented

| Requirement | Where |
|---|---|
| 7-decimal retention in source/normalized data | `vtile-ingest` keeps `f64` end-to-end; normalized GeoJSON round-trips full precision |
| No simplification at zoom ≥ 14 | `SIMPLIFY_BELOW_ZOOM = 14` (`vtile-core/src/config.rs`) |
| Simplification only at low zoom | `simplify.rs` applies Douglas-Peucker-style thinning below z14 only, and never below a zoom-scaled epsilon that would move vertices more than ~half an extent unit at the *target* zoom |
| No silent truncation | quantization is a documented, single, round-to-nearest step at encode time |
| CRS recorded | catalog `crs` field; `assumed_crs` flag when WGS84 was assumed for a missing `.prj` (US-04) |
| Precision QA | decoded tiles are checked against source bbox in tests; visual QA at z10/12/14/16 per release criteria |

## 5. Recommendation

Keep the TRD defaults (parcels 10–16, no simplification ≥ 14). Treat tile
geometry as *display* geometry: accurate to the zoom's resolution table
above, never authoritative for measurement. Expose the retained source
GeoJSON (or a future feature-query API, deferred to V2) for any workflow
that needs the original 7-decimal coordinates.
