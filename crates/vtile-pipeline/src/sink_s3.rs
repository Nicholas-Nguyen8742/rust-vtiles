//! S3 tile sink (enabled with the `aws` feature).
//!
//! Publishes gzip-compressed tiles to `s3://{bucket}/{key_prefix}/{z}/{x}/{y}.pbf`
//! with the object metadata required by TRD §6 and the cache headers required
//! by US-03 (long TTL for static low zooms, short TTL for parcel zooms).
//!
//! The sink exposes the synchronous [`TileSink`] interface used by the tile
//! generator; internally it drives the async SDK on a dedicated runtime with
//! bounded concurrency.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::runtime::Runtime;
use tokio::sync::Semaphore;
use vtile_core::error::{Result as TileResult, TileError};
use vtile_core::sink::{TileObjectMeta, TileSink};
use vtile_core::tilemath::TileId;

const MAX_CONCURRENT_PUTS: usize = 64;

pub struct S3TileSink {
    client: aws_sdk_s3::Client,
    runtime: Runtime,
    bucket: String,
    /// e.g. `tiles/{tenantId}/{layerId}/{tileVersion}`.
    key_prefix: String,
    meta: TileObjectMeta,
    semaphore: Arc<Semaphore>,
}

impl S3TileSink {
    /// Builds a sink from the default AWS config chain (task role on Fargate).
    pub async fn connect(
        bucket: impl Into<String>,
        key_prefix: impl Into<String>,
        meta: TileObjectMeta,
    ) -> Self {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&config);
        Self {
            client,
            runtime: Runtime::new().expect("failed to start sink runtime"),
            bucket: bucket.into(),
            key_prefix: key_prefix.into(),
            meta,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_PUTS)),
        }
    }

    /// TRD/US-03 cache policy: zoom 0–10 are stable geography (24 h),
    /// zoom 11–16 refresh more often (1 h).
    fn cache_control(z: u8) -> &'static str {
        if z <= 10 {
            "public, max-age=86400"
        } else {
            "public, max-age=3600"
        }
    }

    fn put(&self, tile: &TileId, gzipped: Vec<u8>) -> TileResult<()> {
        let key = format!("{}/{}/{}/{}.pbf", self.key_prefix, tile.z, tile.x, tile.y);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let cache_control = Self::cache_control(tile.z);
        let mut metadata = HashMap::new();
        metadata.insert("tenantId".to_string(), self.meta.tenant_id.clone());
        metadata.insert("layerId".to_string(), self.meta.layer_id.clone());
        metadata.insert("tileVersion".to_string(), self.meta.tile_version.clone());
        metadata.insert("sourceFormat".to_string(), self.meta.source_format.clone());
        metadata.insert("crs".to_string(), self.meta.crs.clone());
        metadata.insert("minZoom".to_string(), self.meta.min_zoom.to_string());
        metadata.insert("maxZoom".to_string(), self.meta.max_zoom.to_string());

        let semaphore = self.semaphore.clone();
        self.runtime.block_on(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|e| TileError::Sink(e.to_string()))?;
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(bytes::Bytes::from(gzipped).into())
                .content_type("application/vnd.mapbox-vector-tile")
                .content_encoding("gzip")
                .cache_control(cache_control)
                .set_metadata(Some(metadata))
                .send()
                .await
                .map_err(|e| TileError::Sink(e.to_string()))?;
            Ok::<(), TileError>(())
        })
    }
}

impl TileSink for S3TileSink {
    fn write_tile(&self, tile: &TileId, gzipped: &[u8], _meta: &TileObjectMeta) -> TileResult<()> {
        self.put(tile, gzipped.to_vec())
    }
}
