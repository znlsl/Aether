use std::io::Write;

use aether_data_contracts::repository::usage::{
    parse_usage_body_ref, usage_body_ref, UsageBodyField, UsageCleanupExecutionMode,
    UsageCleanupPreviewCounts, UsageCleanupSummary, UsageCleanupTargets, UsageCleanupWindow,
};
use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};
use serde_json::Value;
use sqlx::Row;
use tracing::warn;

use crate::error::SqlResultExt;
use crate::{DataLayerError, MysqlPool};

const RAW_BODY_PREDICATE: &str = r#"
request_body IS NOT NULL
OR response_body IS NOT NULL
OR provider_request_body IS NOT NULL
OR client_response_body IS NOT NULL
"#;

const COMPRESSED_BODY_PREDICATE: &str = r#"
request_body_compressed IS NOT NULL
OR response_body_compressed IS NOT NULL
OR provider_request_body_compressed IS NOT NULL
OR client_response_body_compressed IS NOT NULL
OR EXISTS (
  SELECT 1 FROM usage_body_blobs
  WHERE usage_body_blobs.request_id = `usage`.request_id
)
OR EXISTS (
  SELECT 1 FROM usage_http_audits
  WHERE usage_http_audits.request_id = `usage`.request_id
    AND (
      usage_http_audits.request_body_ref IS NOT NULL
      OR usage_http_audits.provider_request_body_ref IS NOT NULL
      OR usage_http_audits.response_body_ref IS NOT NULL
      OR usage_http_audits.client_response_body_ref IS NOT NULL
    )
)
"#;

const ALL_BODY_PREDICATE: &str = r#"
request_body IS NOT NULL
OR response_body IS NOT NULL
OR provider_request_body IS NOT NULL
OR client_response_body IS NOT NULL
OR request_body_compressed IS NOT NULL
OR response_body_compressed IS NOT NULL
OR provider_request_body_compressed IS NOT NULL
OR client_response_body_compressed IS NOT NULL
OR EXISTS (
  SELECT 1 FROM usage_body_blobs
  WHERE usage_body_blobs.request_id = `usage`.request_id
)
OR EXISTS (
  SELECT 1 FROM usage_http_audits
  WHERE usage_http_audits.request_id = `usage`.request_id
    AND (
      usage_http_audits.request_body_ref IS NOT NULL
      OR usage_http_audits.provider_request_body_ref IS NOT NULL
      OR usage_http_audits.response_body_ref IS NOT NULL
      OR usage_http_audits.client_response_body_ref IS NOT NULL
    )
)
"#;

const INLINE_OR_COMPRESSED_BODY_PREDICATE: &str = r#"
request_body IS NOT NULL
OR response_body IS NOT NULL
OR provider_request_body IS NOT NULL
OR client_response_body IS NOT NULL
OR request_body_compressed IS NOT NULL
OR response_body_compressed IS NOT NULL
OR provider_request_body_compressed IS NOT NULL
OR client_response_body_compressed IS NOT NULL
"#;

const HEADER_PREDICATE: &str = r#"
request_headers IS NOT NULL
OR response_headers IS NOT NULL
OR provider_request_headers IS NOT NULL
OR client_response_headers IS NOT NULL
OR EXISTS (
  SELECT 1 FROM usage_http_audits
  WHERE usage_http_audits.request_id = `usage`.request_id
    AND (
      usage_http_audits.request_headers IS NOT NULL
      OR usage_http_audits.response_headers IS NOT NULL
      OR usage_http_audits.provider_request_headers IS NOT NULL
      OR usage_http_audits.client_response_headers IS NOT NULL
    )
)
"#;

const LEGACY_BODY_REF_PREDICATE: &str = r#"
request_metadata IS NOT NULL
AND JSON_VALID(request_metadata)
AND JSON_CONTAINS_PATH(
  CASE WHEN JSON_VALID(request_metadata) THEN request_metadata ELSE JSON_OBJECT() END,
  'one',
  '$.request_body_ref',
  '$.provider_request_body_ref',
  '$.response_body_ref',
  '$.client_response_body_ref'
) = 1
"#;

const DETAIL_BODY_PREDICATE: &str = r#"
request_body IS NOT NULL
OR response_body IS NOT NULL
OR provider_request_body IS NOT NULL
OR client_response_body IS NOT NULL
OR request_body_compressed IS NOT NULL
OR response_body_compressed IS NOT NULL
OR provider_request_body_compressed IS NOT NULL
OR client_response_body_compressed IS NOT NULL
OR (
  request_metadata IS NOT NULL
  AND JSON_VALID(request_metadata)
  AND JSON_CONTAINS_PATH(
    CASE WHEN JSON_VALID(request_metadata) THEN request_metadata ELSE JSON_OBJECT() END,
    'one',
    '$.request_body_ref',
    '$.provider_request_body_ref',
    '$.response_body_ref',
    '$.client_response_body_ref'
  ) = 1
)
"#;

#[derive(Debug)]
struct CleanupRow {
    id: String,
    request_id: String,
}

#[derive(Debug)]
struct BodyRow {
    id: String,
    request_id: String,
    request_body: Option<Value>,
    request_body_compressed: Option<Vec<u8>>,
    provider_request_body: Option<Value>,
    provider_request_body_compressed: Option<Vec<u8>>,
    response_body: Option<Value>,
    response_body_compressed: Option<Vec<u8>>,
    client_response_body: Option<Value>,
    client_response_body_compressed: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
struct DetachedRefs {
    request_body_ref: Option<String>,
    provider_request_body_ref: Option<String>,
    response_body_ref: Option<String>,
    client_response_body_ref: Option<String>,
}

impl DetachedRefs {
    fn any_present(&self) -> bool {
        self.request_body_ref.is_some()
            || self.provider_request_body_ref.is_some()
            || self.response_body_ref.is_some()
            || self.client_response_body_ref.is_some()
    }
}

struct DetachedBlob {
    body_ref: String,
    body_field: &'static str,
    payload_gzip: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyCleanupKind {
    Raw,
    Compressed,
    All,
}

impl BodyCleanupKind {
    fn predicate(self) -> &'static str {
        match self {
            Self::Raw => RAW_BODY_PREDICATE,
            Self::Compressed => COMPRESSED_BODY_PREDICATE,
            Self::All => ALL_BODY_PREDICATE,
        }
    }

    fn clears_detached(self) -> bool {
        self != Self::Raw
    }
}

pub(crate) async fn cleanup_usage(
    pool: &MysqlPool,
    window: &UsageCleanupWindow,
    batch_size: usize,
    auto_delete_expired_keys: bool,
    targets: UsageCleanupTargets,
    mode: UsageCleanupExecutionMode,
) -> Result<UsageCleanupSummary, DataLayerError> {
    if batch_size == 0 || !targets.any_selected() {
        return Ok(UsageCleanupSummary::default());
    }

    if mode == UsageCleanupExecutionMode::BeforeNowBodyFields {
        let body_externalized = if targets.detail_body {
            cleanup_body_fields(
                pool,
                window.detail_cutoff,
                None,
                batch_size,
                BodyCleanupKind::Raw,
            )
            .await?
        } else {
            0
        };
        let body_cleaned = if targets.compressed_body {
            cleanup_body_fields(
                pool,
                window.compressed_cutoff,
                None,
                batch_size,
                BodyCleanupKind::Compressed,
            )
            .await?
        } else {
            0
        };
        return Ok(UsageCleanupSummary {
            body_externalized,
            body_cleaned,
            ..UsageCleanupSummary::default()
        });
    }

    let records_deleted = if targets.records {
        delete_old_usage_records(pool, window.log_cutoff, batch_size).await?
    } else {
        0
    };
    let record_cutoff = targets.records.then_some(window.log_cutoff);
    let header_cleaned = if targets.headers {
        cleanup_headers(pool, window.header_cutoff, record_cutoff, batch_size).await?
    } else {
        0
    };
    let body_cleaned = if targets.compressed_body {
        cleanup_body_fields(
            pool,
            window.compressed_cutoff,
            record_cutoff,
            batch_size,
            BodyCleanupKind::All,
        )
        .await?
    } else {
        0
    };
    let detail_newer_than = detail_body_newer_than(window, targets);
    let legacy_body_refs_migrated = if targets.detail_body {
        migrate_legacy_body_refs(pool, window.detail_cutoff, detail_newer_than, batch_size).await?
    } else {
        0
    };
    let body_externalized = if targets.detail_body {
        externalize_detail_bodies(pool, window.detail_cutoff, detail_newer_than, batch_size).await?
    } else {
        0
    };
    let keys_cleaned = if targets.expired_keys {
        match cleanup_expired_api_keys(pool, auto_delete_expired_keys).await {
            Ok(count) => count,
            Err(err) => {
                warn!(error = %err, "MySQL usage cleanup expired api key sweep failed");
                0
            }
        }
    } else {
        0
    };

    Ok(UsageCleanupSummary {
        body_externalized,
        legacy_body_refs_migrated,
        body_cleaned,
        header_cleaned,
        keys_cleaned,
        records_deleted,
        cost_reservations_deleted: 0,
        request_admissions_deleted: 0,
    })
}

pub(crate) async fn preview_usage_cleanup(
    pool: &MysqlPool,
    window: &UsageCleanupWindow,
    targets: UsageCleanupTargets,
    mode: UsageCleanupExecutionMode,
) -> Result<UsageCleanupPreviewCounts, DataLayerError> {
    if mode == UsageCleanupExecutionMode::BeforeNowBodyFields {
        let detail = if targets.detail_body {
            count_candidates(pool, RAW_BODY_PREDICATE, window.detail_cutoff, None).await?
        } else {
            0
        };
        let compressed = if targets.compressed_body {
            count_candidates(
                pool,
                COMPRESSED_BODY_PREDICATE,
                window.compressed_cutoff,
                None,
            )
            .await?
        } else {
            0
        };
        return Ok(UsageCleanupPreviewCounts {
            detail,
            compressed,
            header: 0,
            log: 0,
        });
    }

    let record_cutoff = targets.records.then_some(window.log_cutoff);
    let detail = if targets.detail_body {
        count_candidates(
            pool,
            DETAIL_BODY_PREDICATE,
            window.detail_cutoff,
            detail_body_newer_than(window, targets),
        )
        .await?
    } else {
        0
    };
    let compressed = if targets.compressed_body {
        count_candidates(
            pool,
            ALL_BODY_PREDICATE,
            window.compressed_cutoff,
            record_cutoff,
        )
        .await?
    } else {
        0
    };
    let header = if targets.headers {
        count_candidates(pool, HEADER_PREDICATE, window.header_cutoff, record_cutoff).await?
    } else {
        0
    };
    let log = if targets.records {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM `usage` WHERE created_at_unix_ms < ?")
                .bind(window.log_cutoff.timestamp())
                .fetch_one(pool)
                .await
                .map_sql_err()?;
        u64::try_from(count).unwrap_or(0)
    } else {
        0
    };

    Ok(UsageCleanupPreviewCounts {
        detail,
        compressed,
        header,
        log,
    })
}

fn detail_body_newer_than(
    window: &UsageCleanupWindow,
    targets: UsageCleanupTargets,
) -> Option<DateTime<Utc>> {
    [
        targets.compressed_body.then_some(window.compressed_cutoff),
        targets.records.then_some(window.log_cutoff),
    ]
    .into_iter()
    .flatten()
    .max()
}

fn invalid_window(cutoff: DateTime<Utc>, newer_than: Option<DateTime<Utc>>) -> bool {
    matches!(newer_than, Some(value) if value >= cutoff)
}

async fn count_candidates(
    pool: &MysqlPool,
    predicate: &str,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
) -> Result<u64, DataLayerError> {
    if invalid_window(cutoff, newer_than) {
        return Ok(0);
    }
    let sql = format!(
        r#"
SELECT COUNT(*)
FROM `usage`
WHERE created_at_unix_ms < ?
  AND (? IS NULL OR created_at_unix_ms >= ?)
  AND ({predicate})
"#
    );
    let newer_than = newer_than.map(|value| value.timestamp());
    let count: i64 = sqlx::query_scalar(&sql)
        .bind(cutoff.timestamp())
        .bind(newer_than)
        .bind(newer_than)
        .fetch_one(pool)
        .await
        .map_sql_err()?;
    Ok(u64::try_from(count).unwrap_or(0))
}

async fn fetch_cleanup_rows(
    pool: &MysqlPool,
    predicate: &str,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
    batch_size: usize,
) -> Result<Vec<CleanupRow>, DataLayerError> {
    if invalid_window(cutoff, newer_than) {
        return Ok(Vec::new());
    }
    let sql = format!(
        r#"
SELECT id, request_id
FROM `usage`
WHERE created_at_unix_ms < ?
  AND (? IS NULL OR created_at_unix_ms >= ?)
  AND ({predicate})
ORDER BY created_at_unix_ms ASC, id ASC
LIMIT ?
"#
    );
    let newer_than = newer_than.map(|value| value.timestamp());
    sqlx::query(&sql)
        .bind(cutoff.timestamp())
        .bind(newer_than)
        .bind(newer_than)
        .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await
        .map_sql_err()?
        .into_iter()
        .map(|row| {
            Ok(CleanupRow {
                id: row.try_get("id").map_sql_err()?,
                request_id: row.try_get("request_id").map_sql_err()?,
            })
        })
        .collect()
}

async fn delete_old_usage_records(
    pool: &MysqlPool,
    cutoff: DateTime<Utc>,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    let mut total = 0usize;
    loop {
        let rows = fetch_cleanup_rows(pool, "1 = 1", cutoff, None, batch_size).await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        let mut tx = pool.begin().await.map_sql_err()?;
        let mut deleted = 0usize;
        for row in rows {
            deleted += usize::try_from(
                sqlx::query("DELETE FROM `usage` WHERE id = ?")
                    .bind(row.id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?
                    .rows_affected(),
            )
            .unwrap_or(usize::MAX);
        }
        tx.commit().await.map_sql_err()?;
        total = total.saturating_add(deleted);
        if row_count < batch_size {
            break;
        }
    }
    Ok(total)
}

async fn cleanup_headers(
    pool: &MysqlPool,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    if invalid_window(cutoff, newer_than) {
        warn!(%cutoff, ?newer_than, "MySQL usage header cleanup skipped due to invalid window");
        return Ok(0);
    }
    let mut total = 0usize;
    loop {
        let rows =
            fetch_cleanup_rows(pool, HEADER_PREDICATE, cutoff, newer_than, batch_size).await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        let mut tx = pool.begin().await.map_sql_err()?;
        for row in rows {
            sqlx::query(
                r#"
UPDATE `usage`
SET request_headers = NULL,
    response_headers = NULL,
    provider_request_headers = NULL,
    client_response_headers = NULL
WHERE id = ?
"#,
            )
            .bind(&row.id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
            sqlx::query(
                r#"
UPDATE usage_http_audits
SET request_headers = NULL,
    response_headers = NULL,
    provider_request_headers = NULL,
    client_response_headers = NULL,
    updated_at = UNIX_TIMESTAMP()
WHERE request_id = ?
"#,
            )
            .bind(&row.request_id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
            delete_empty_http_audit(&mut tx, &row.request_id).await?;
        }
        tx.commit().await.map_sql_err()?;
        total = total.saturating_add(row_count);
        if row_count < batch_size {
            break;
        }
    }
    Ok(total)
}

async fn cleanup_body_fields(
    pool: &MysqlPool,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
    batch_size: usize,
    kind: BodyCleanupKind,
) -> Result<usize, DataLayerError> {
    if invalid_window(cutoff, newer_than) {
        warn!(%cutoff, ?newer_than, "MySQL usage body cleanup skipped due to invalid window");
        return Ok(0);
    }
    let mut total = 0usize;
    loop {
        let rows =
            fetch_cleanup_rows(pool, kind.predicate(), cutoff, newer_than, batch_size).await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        let mut tx = pool.begin().await.map_sql_err()?;
        for row in rows {
            let update = if kind == BodyCleanupKind::All {
                r#"
UPDATE `usage`
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE id = ?
"#
            } else if kind == BodyCleanupKind::Compressed {
                r#"
UPDATE `usage`
SET request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE id = ?
"#
            } else {
                r#"
UPDATE `usage`
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL
WHERE id = ?
"#
            };
            sqlx::query(update)
                .bind(&row.id)
                .execute(&mut *tx)
                .await
                .map_sql_err()?;
            if kind.clears_detached() {
                sqlx::query("DELETE FROM usage_body_blobs WHERE request_id = ?")
                    .bind(&row.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                sqlx::query(
                    r#"
UPDATE usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    body_capture_mode = 'none',
    updated_at = UNIX_TIMESTAMP()
WHERE request_id = ?
"#,
                )
                .bind(&row.request_id)
                .execute(&mut *tx)
                .await
                .map_sql_err()?;
                delete_empty_http_audit(&mut tx, &row.request_id).await?;
            }
        }
        tx.commit().await.map_sql_err()?;
        total = total.saturating_add(row_count);
        if row_count < batch_size {
            break;
        }
    }
    Ok(total)
}

async fn delete_empty_http_audit(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    request_id: &str,
) -> Result<(), DataLayerError> {
    sqlx::query(
        r#"
DELETE FROM usage_http_audits
WHERE request_id = ?
  AND request_headers IS NULL
  AND response_headers IS NULL
  AND provider_request_headers IS NULL
  AND client_response_headers IS NULL
  AND request_body_ref IS NULL
  AND provider_request_body_ref IS NULL
  AND response_body_ref IS NULL
  AND client_response_body_ref IS NULL
"#,
    )
    .bind(request_id)
    .execute(&mut **tx)
    .await
    .map_sql_err()?;
    Ok(())
}

async fn migrate_legacy_body_refs(
    pool: &MysqlPool,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    if invalid_window(cutoff, newer_than) {
        warn!(%cutoff, ?newer_than, "MySQL usage legacy body-ref migration skipped due to invalid window");
        return Ok(0);
    }
    let mut total = 0usize;
    loop {
        let rows = fetch_cleanup_rows(
            pool,
            LEGACY_BODY_REF_PREDICATE,
            cutoff,
            newer_than,
            batch_size,
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        let mut tx = pool.begin().await.map_sql_err()?;
        let mut migrated = 0usize;
        for row in rows {
            let metadata: Option<String> =
                sqlx::query_scalar("SELECT request_metadata FROM `usage` WHERE id = ? LIMIT 1")
                    .bind(&row.id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_sql_err()?
                    .flatten();
            let Some((refs, metadata)) =
                legacy_body_ref_plan(&row.request_id, metadata.as_deref())?
            else {
                continue;
            };
            if refs.any_present() {
                upsert_http_audit_refs(&mut tx, &row.request_id, &refs).await?;
            }
            let updated = sqlx::query(
                r#"
UPDATE `usage`
SET request_metadata = ?,
    updated_at_unix_secs = UNIX_TIMESTAMP()
WHERE id = ?
"#,
            )
            .bind(metadata)
            .bind(row.id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?
            .rows_affected();
            if updated > 0 {
                migrated += 1;
            }
        }
        tx.commit().await.map_sql_err()?;
        total = total.saturating_add(migrated);
        if row_count < batch_size || migrated == 0 {
            break;
        }
    }
    Ok(total)
}

fn legacy_body_ref_plan(
    request_id: &str,
    metadata: Option<&str>,
) -> Result<Option<(DetachedRefs, Option<String>)>, DataLayerError> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(metadata).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("invalid usage request_metadata JSON: {err}"))
    })?;
    let Value::Object(mut object) = value else {
        return Ok(None);
    };
    let mut refs = DetachedRefs::default();
    let mut removed = false;
    for field in [
        UsageBodyField::RequestBody,
        UsageBodyField::ProviderRequestBody,
        UsageBodyField::ResponseBody,
        UsageBodyField::ClientResponseBody,
    ] {
        let Some(value) = object.remove(field.as_ref_key()) else {
            continue;
        };
        removed = true;
        let parsed = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(parse_usage_body_ref)
            .filter(|(parsed_request_id, parsed_field)| {
                parsed_request_id == request_id && *parsed_field == field
            })
            .map(|(parsed_request_id, parsed_field)| {
                usage_body_ref(&parsed_request_id, parsed_field)
            });
        set_ref(&mut refs, field, parsed);
    }
    if !removed {
        return Ok(None);
    }
    let metadata = if object.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&Value::Object(object)).map_err(|err| {
                DataLayerError::UnexpectedValue(format!(
                    "failed to serialize request_metadata: {err}"
                ))
            })?,
        )
    };
    Ok(Some((refs, metadata)))
}

async fn externalize_detail_bodies(
    pool: &MysqlPool,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    if invalid_window(cutoff, newer_than) {
        warn!(%cutoff, ?newer_than, "MySQL usage body externalization skipped due to invalid window");
        return Ok(0);
    }
    let batch_size = batch_size.clamp(1, 25);
    let mut total = 0usize;
    loop {
        let rows = fetch_body_rows(pool, cutoff, newer_than, batch_size).await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        let mut externalized = 0usize;
        for row in rows {
            let (blobs, refs) = build_detached_bodies(&row)?;
            let mut tx = pool.begin().await.map_sql_err()?;
            for blob in blobs {
                sqlx::query(
                    r#"
INSERT INTO usage_body_blobs (body_ref, request_id, body_field, payload_gzip)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  request_id = VALUES(request_id),
  body_field = VALUES(body_field),
  payload_gzip = VALUES(payload_gzip),
  updated_at = UNIX_TIMESTAMP()
"#,
                )
                .bind(blob.body_ref)
                .bind(&row.request_id)
                .bind(blob.body_field)
                .bind(blob.payload_gzip)
                .execute(&mut *tx)
                .await
                .map_sql_err()?;
            }
            if refs.any_present() {
                upsert_http_audit_refs(&mut tx, &row.request_id, &refs).await?;
            }
            let updated = sqlx::query(
                r#"
UPDATE `usage`
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE id = ?
"#,
            )
            .bind(row.id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?
            .rows_affected();
            tx.commit().await.map_sql_err()?;
            if updated > 0 {
                externalized += 1;
            }
        }
        total = total.saturating_add(externalized);
        if row_count < batch_size || externalized == 0 {
            break;
        }
    }
    Ok(total)
}

async fn fetch_body_rows(
    pool: &MysqlPool,
    cutoff: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
    batch_size: usize,
) -> Result<Vec<BodyRow>, DataLayerError> {
    let newer_than = newer_than.map(|value| value.timestamp());
    let sql = format!(
        r#"
SELECT id,
       request_id,
       CAST(request_body AS CHAR) AS request_body,
       request_body_compressed,
       CAST(provider_request_body AS CHAR) AS provider_request_body,
       provider_request_body_compressed,
       CAST(response_body AS CHAR) AS response_body,
       response_body_compressed,
       CAST(client_response_body AS CHAR) AS client_response_body,
       client_response_body_compressed
FROM `usage`
WHERE created_at_unix_ms < ?
  AND (? IS NULL OR created_at_unix_ms >= ?)
  AND ({INLINE_OR_COMPRESSED_BODY_PREDICATE})
ORDER BY created_at_unix_ms ASC, id ASC
LIMIT ?
"#
    );
    sqlx::query(&sql)
        .bind(cutoff.timestamp())
        .bind(newer_than)
        .bind(newer_than)
        .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await
        .map_sql_err()?
        .into_iter()
        .map(|row| {
            Ok(BodyRow {
                id: row.try_get("id").map_sql_err()?,
                request_id: row.try_get("request_id").map_sql_err()?,
                request_body: parse_optional_json(row.try_get("request_body").map_sql_err()?)?,
                request_body_compressed: row.try_get("request_body_compressed").map_sql_err()?,
                provider_request_body: parse_optional_json(
                    row.try_get("provider_request_body").map_sql_err()?,
                )?,
                provider_request_body_compressed: row
                    .try_get("provider_request_body_compressed")
                    .map_sql_err()?,
                response_body: parse_optional_json(row.try_get("response_body").map_sql_err()?)?,
                response_body_compressed: row.try_get("response_body_compressed").map_sql_err()?,
                client_response_body: parse_optional_json(
                    row.try_get("client_response_body").map_sql_err()?,
                )?,
                client_response_body_compressed: row
                    .try_get("client_response_body_compressed")
                    .map_sql_err()?,
            })
        })
        .collect()
}

fn parse_optional_json(raw: Option<String>) -> Result<Option<Value>, DataLayerError> {
    raw.map(|raw| {
        serde_json::from_str(&raw).map_err(|err| {
            DataLayerError::UnexpectedValue(format!("invalid inline usage body JSON: {err}"))
        })
    })
    .transpose()
}

fn build_detached_bodies(
    row: &BodyRow,
) -> Result<(Vec<DetachedBlob>, DetachedRefs), DataLayerError> {
    let mut blobs = Vec::new();
    let mut refs = DetachedRefs::default();
    add_detached_body(
        &mut blobs,
        &mut refs,
        &row.request_id,
        UsageBodyField::RequestBody,
        row.request_body.as_ref(),
        row.request_body_compressed.as_deref(),
    )?;
    add_detached_body(
        &mut blobs,
        &mut refs,
        &row.request_id,
        UsageBodyField::ProviderRequestBody,
        row.provider_request_body.as_ref(),
        row.provider_request_body_compressed.as_deref(),
    )?;
    add_detached_body(
        &mut blobs,
        &mut refs,
        &row.request_id,
        UsageBodyField::ResponseBody,
        row.response_body.as_ref(),
        row.response_body_compressed.as_deref(),
    )?;
    add_detached_body(
        &mut blobs,
        &mut refs,
        &row.request_id,
        UsageBodyField::ClientResponseBody,
        row.client_response_body.as_ref(),
        row.client_response_body_compressed.as_deref(),
    )?;
    Ok((blobs, refs))
}

fn add_detached_body(
    blobs: &mut Vec<DetachedBlob>,
    refs: &mut DetachedRefs,
    request_id: &str,
    field: UsageBodyField,
    raw: Option<&Value>,
    compressed: Option<&[u8]>,
) -> Result<(), DataLayerError> {
    let payload_gzip = match raw {
        Some(value) => Some(compress_json(value)?),
        None => compressed.map(ToOwned::to_owned),
    };
    let Some(payload_gzip) = payload_gzip else {
        return Ok(());
    };
    let body_ref = usage_body_ref(request_id, field);
    blobs.push(DetachedBlob {
        body_ref: body_ref.clone(),
        body_field: field.as_storage_field(),
        payload_gzip,
    });
    set_ref(refs, field, Some(body_ref));
    Ok(())
}

fn compress_json(value: &Value) -> Result<Vec<u8>, DataLayerError> {
    let bytes = serde_json::to_vec(value).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("failed to serialize usage body: {err}"))
    })?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(&bytes).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("failed to gzip usage body: {err}"))
    })?;
    encoder.finish().map_err(|err| {
        DataLayerError::UnexpectedValue(format!("failed to finish usage body gzip: {err}"))
    })
}

fn set_ref(refs: &mut DetachedRefs, field: UsageBodyField, value: Option<String>) {
    match field {
        UsageBodyField::RequestBody => refs.request_body_ref = value,
        UsageBodyField::ProviderRequestBody => refs.provider_request_body_ref = value,
        UsageBodyField::ResponseBody => refs.response_body_ref = value,
        UsageBodyField::ClientResponseBody => refs.client_response_body_ref = value,
    }
}

async fn upsert_http_audit_refs(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    request_id: &str,
    refs: &DetachedRefs,
) -> Result<(), DataLayerError> {
    sqlx::query(
        r#"
INSERT INTO usage_http_audits (
  request_id,
  request_body_ref,
  provider_request_body_ref,
  response_body_ref,
  client_response_body_ref,
  body_capture_mode
)
VALUES (?, ?, ?, ?, ?, 'ref_backed')
ON DUPLICATE KEY UPDATE
  request_body_ref = COALESCE(VALUES(request_body_ref), request_body_ref),
  provider_request_body_ref = COALESCE(VALUES(provider_request_body_ref), provider_request_body_ref),
  response_body_ref = COALESCE(VALUES(response_body_ref), response_body_ref),
  client_response_body_ref = COALESCE(VALUES(client_response_body_ref), client_response_body_ref),
  body_capture_mode = 'ref_backed',
  updated_at = UNIX_TIMESTAMP()
"#,
    )
    .bind(request_id)
    .bind(refs.request_body_ref.as_deref())
    .bind(refs.provider_request_body_ref.as_deref())
    .bind(refs.response_body_ref.as_deref())
    .bind(refs.client_response_body_ref.as_deref())
    .execute(&mut **tx)
    .await
    .map_sql_err()?;
    Ok(())
}

async fn cleanup_expired_api_keys(
    pool: &MysqlPool,
    auto_delete_expired_keys: bool,
) -> Result<usize, DataLayerError> {
    let now = Utc::now().timestamp();
    let rows = sqlx::query(
        r#"
SELECT id, auto_delete_on_expiry
FROM api_keys
WHERE expires_at <= ?
  AND is_active = TRUE
ORDER BY expires_at ASC, id ASC
"#,
    )
    .bind(now)
    .fetch_all(pool)
    .await
    .map_sql_err()?;
    let mut cleaned = 0usize;
    for row in rows {
        let id: String = row.try_get("id").map_sql_err()?;
        let auto_delete = row
            .try_get::<Option<i8>, _>("auto_delete_on_expiry")
            .map_sql_err()?
            .map(|value| value != 0)
            .unwrap_or(auto_delete_expired_keys);
        let mut tx = pool.begin().await.map_sql_err()?;
        let affected = if auto_delete {
            sqlx::query(
                "UPDATE wallets SET status = 'disabled', updated_at = ? WHERE api_key_id = ? AND status <> 'disabled'",
            )
            .bind(now)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
            sqlx::query("DELETE FROM api_keys WHERE id = ?")
                .bind(&id)
                .execute(&mut *tx)
                .await
                .map_sql_err()?
                .rows_affected()
        } else {
            sqlx::query(
                "UPDATE api_keys SET is_active = FALSE, updated_at = ? WHERE id = ? AND is_active = TRUE",
            )
            .bind(now)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?
            .rows_affected()
        };
        tx.commit().await.map_sql_err()?;
        if affected > 0 {
            cleaned += 1;
        }
    }
    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;
    use serde_json::json;

    use super::{compress_json, legacy_body_ref_plan};

    #[test]
    fn mysql_cleanup_legacy_body_ref_plan_preserves_unrelated_metadata() {
        let metadata = json!({
            "trace": "kept",
            "request_body_ref": "usage://request/request-1/request_body",
            "response_body_ref": "usage://request/other/response_body"
        })
        .to_string();
        let (refs, metadata) = legacy_body_ref_plan("request-1", Some(&metadata))
            .expect("legacy plan should build")
            .expect("legacy refs should be present");
        assert_eq!(
            refs.request_body_ref.as_deref(),
            Some("usage://request/request-1/request_body")
        );
        assert!(refs.response_body_ref.is_none());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                metadata.as_deref().expect("trace metadata should remain")
            )
            .expect("metadata should remain valid JSON"),
            json!({"trace": "kept"})
        );
    }

    #[test]
    fn mysql_cleanup_gzip_payload_round_trips() {
        let value = json!({"hello": "world"});
        let payload = compress_json(&value).expect("body should compress");
        let mut decoder = GzDecoder::new(payload.as_slice());
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .expect("body should decompress");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded)
                .expect("decoded body should be JSON"),
            value
        );
    }
}
