# Mapbox Vector Tile (MVT) v2 — Encoding Notes

How this workspace encodes MVT tiles, and what the format looks like on the
wire. The encoder lives in `crates/vtile-core/src/mvt/` and is written
directly against the protobuf wire format (no generated code), keeping the
pipeline dependency-free of protobuf compilers.

## 1. Wire format basics (protobuf)

Every MVT message is a sequence of `(tag, value)` fields:

```text
tag = (field_number << 3) | wire_type
```

Wire types used:

| Wire type | Meaning | Used for |
|---|---|---|
| `0` VARINT | base-128 varint | ids, enums, packed ints |
| `2` LEN | length-delimited | nested messages, strings, packed arrays |

Varints encode 7 bits per byte, MSB = continuation. Signed integers in
geometry parameters are **zigzag** encoded: `(n << 1) ^ (n >> 31)`.

## 2. Message structure (vector_tile.proto)

```text
message Tile {
  repeated Layer layers = 3;

  message Layer {
    required string name     = 1;
    repeated Feature features = 2;
    repeated string keys     = 3;   // interned attribute names
    repeated Value  values   = 4;   // interned attribute values
    optional uint32 extent   = 5;   // 4096 by default
    required uint32 version  = 15;  // 2
  }

  message Feature {
    optional uint64 id = 1;
    repeated uint32 tags = 2 [packed];   // k,v index pairs into keys/values
    optional GeomType type = 3;          // UNKNOWN=0 POINT=1 LINESTRING=2 POLYGON=3
    repeated uint32 geometry = 4 [packed]; // command stream
  }

  message Value {
    string_value=1, float_value=2, double_value=3, int_value=4,
    uint_value=5, sint_value=6, bool_value=7   // exactly one set
  }
}
```

Notes as implemented:

- **Keys/values are interned per layer** (`MvtLayer` keeps a map → index).
  Tags reference them pairwise: `[keyIdx, valueIdx, keyIdx, valueIdx, …]`.
- **One layer per tile** for MVP (`layer_name` from config, e.g.
  `parcel_boundary`). Composite multi-layer tiles (TRD US-09) are a matter of
  appending more `Layer` messages — the encoder already supports it.
- **Value typing:** JSON numbers become `int_value` when integral and
  `double_value` otherwise; strings → `string_value`; booleans →
  `bool_value`. This matches how renderers style on attributes.

## 3. Geometry commands

Geometry is a packed varint stream of commands. Command integer:

```text
cmd_int = (command_id & 0x7) | (parameter_count << 3)
```

| Command | id | Meaning |
|---|---|---|
| MoveTo | 1 | pen up + move (starts ring/line/point) |
| LineTo | 2 | pen down + draw |
| ClosePath | 7 | close ring (no parameters) |

Parameters are **zigzag deltas** from the current pen position. The pen
starts at `(0, 0)` per feature; coordinates are tile-local integers.

Shapes:

- **Point/MultiPoint:** `MoveTo(count)` with count points.
- **LineString:** `MoveTo(1)` + `LineTo(n-1)` per line.
- **Polygon:** `MoveTo(1)` + `LineTo(n-2)` + `ClosePath` per ring. Exterior
  and interior rings are emitted in one continuous stream; renderers pair
  holes by signed area (winding), not by explicit grouping.

Coordinate space:

- Origin at tile **top-left**, x → right, **y → down** (screen space).
- Extent 4096 units per tile edge; features may spill into
  `[-buffer, extent + buffer]` (buffer = 256) so strokes don't gap at edges.
- Conversion: lon/lat → world `(0..1, 0..1)` via Web Mercator → tile-local
  integer (`TileTransform`). See `docs/PRECISION.md` for the resulting
  precision per zoom.

## 4. Polygon orientation

The MVT spec recommends clockwise exterior rings (positive signed area in
screen coordinates) and counter-clockwise holes. Renderers (MapLibre GL,
Mapbox GL) are tolerant in practice, but this pipeline normalizes winding
during repair (`vtile-ingest::repair`) before encoding, so emitted rings
follow the spec convention regardless of source winding order.

## 5. Compression and delivery

Tiles are gzip-compressed (`flate2`, level 6 by default) immediately after
encoding and stored/served as `.pbf` with:

```text
Content-Type: application/vnd.mapbox-vector-tile
Content-Encoding: gzip
```

## 6. Verification / QA

- `mvt::decode::decode_gzipped_tile` round-trips the wire format
  structurally (layer name, version, extent, keys, per-feature geometry
  counts). It backs unit tests and the `vtile inspect-tile` CLI subcommand.
- End-to-end tests assert `version == 2`, `extent == 4096`, expected
  geometry types, and property allow/deny behavior per tile.
- Visual QA (TRD release criteria): load tiles in MapLibre GL JS against the
  source GeoJSON at zooms 10/12/14/16.

## 7. Why a hand-rolled encoder?

- Zero native/protobuf-toolchain dependencies → the whole pipeline builds
  with stock `cargo build` (important for the Fargate image and CI).
- The MVT proto surface is tiny; the encoder is ~500 lines and covered by
  round-trip decode tests.
- Swapping in the `mvt` crate or `geozero` MVT writer is possible without
  touching callers: `generate_tiles` only needs a byte-producing sink.
