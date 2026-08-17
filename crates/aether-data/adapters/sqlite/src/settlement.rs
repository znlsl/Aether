use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

use aether_data_contracts::repository::settlement::{
    finite_wallet_available_usd, plan_finite_wallet_debit, settlement_billable_cost_usd,
    settlement_billing_status_for_usage_status, ReconcileUsagePolicyCostInput,
    ReleaseUsagePolicyRequestAdmissionInput, ReserveUsagePolicyCostInput,
    ReserveUsagePolicyCostOutcome, ReserveUsagePolicyRequestInput,
    ReserveUsagePolicyRequestOutcome, SettlementWriteRepository, StoredUsagePolicyCostReservation,
    StoredUsagePolicyRequestAdmission, StoredUsageSettlement, UsagePolicyCostReservationState,
    UsagePolicyRequestAdmissionState, UsageSettlementInput, SETTLEMENT_EPSILON_USD,
};
use aether_data_contracts::DataLayerError;

use crate::error::SqlResultExt;
use crate::{sqlite_optional_real, sqlite_real, SqlitePool};

const FIND_USAGE_FOR_SETTLEMENT_SQL: &str = r#"
SELECT
  usage_record.request_id,
  COALESCE(usage_settlement_snapshots.wallet_id, usage_record.wallet_id) AS wallet_id,
  COALESCE(usage_settlement_snapshots.billing_status, usage_record.billing_status) AS billing_status,
  COALESCE(
    usage_settlement_snapshots.wallet_balance_before,
    usage_record.wallet_balance_before
  ) AS wallet_balance_before,
  COALESCE(
    usage_settlement_snapshots.wallet_balance_after,
    usage_record.wallet_balance_after
  ) AS wallet_balance_after,
  COALESCE(
    usage_settlement_snapshots.wallet_recharge_balance_before,
    usage_record.wallet_recharge_balance_before
  ) AS wallet_recharge_balance_before,
  COALESCE(
    usage_settlement_snapshots.wallet_recharge_balance_after,
    usage_record.wallet_recharge_balance_after
  ) AS wallet_recharge_balance_after,
  COALESCE(
    usage_settlement_snapshots.wallet_gift_balance_before,
    usage_record.wallet_gift_balance_before
  ) AS wallet_gift_balance_before,
  COALESCE(
    usage_settlement_snapshots.wallet_gift_balance_after,
    usage_record.wallet_gift_balance_after
  ) AS wallet_gift_balance_after,
  CAST(usage_settlement_snapshots.provider_monthly_used_usd AS REAL) AS provider_monthly_used_usd,
  usage_record.provider_id,
  COALESCE(usage_settlement_snapshots.finalized_at, usage_record.finalized_at) AS finalized_at_unix_secs
FROM "usage" AS usage_record
LEFT JOIN usage_settlement_snapshots
  ON usage_settlement_snapshots.request_id = usage_record.request_id
WHERE usage_record.request_id = ?
"#;

const FINALIZE_USAGE_BILLING_SQL: &str = r#"
UPDATE "usage"
SET
  billing_status = ?,
  finalized_at = COALESCE(finalized_at, ?)
WHERE request_id = ?
"#;

const UPSERT_USAGE_SETTLEMENT_SNAPSHOT_SQL: &str = r#"
INSERT INTO usage_settlement_snapshots (
  request_id,
  billing_status,
  wallet_id,
  wallet_balance_before,
  wallet_balance_after,
  wallet_recharge_balance_before,
  wallet_recharge_balance_after,
  wallet_gift_balance_before,
  wallet_gift_balance_after,
  provider_monthly_used_usd,
  finalized_at,
  created_at,
  updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (request_id)
DO UPDATE SET
  billing_status = excluded.billing_status,
  wallet_id = COALESCE(excluded.wallet_id, usage_settlement_snapshots.wallet_id),
  wallet_balance_before = COALESCE(
    excluded.wallet_balance_before,
    usage_settlement_snapshots.wallet_balance_before
  ),
  wallet_balance_after = COALESCE(
    excluded.wallet_balance_after,
    usage_settlement_snapshots.wallet_balance_after
  ),
  wallet_recharge_balance_before = COALESCE(
    excluded.wallet_recharge_balance_before,
    usage_settlement_snapshots.wallet_recharge_balance_before
  ),
  wallet_recharge_balance_after = COALESCE(
    excluded.wallet_recharge_balance_after,
    usage_settlement_snapshots.wallet_recharge_balance_after
  ),
  wallet_gift_balance_before = COALESCE(
    excluded.wallet_gift_balance_before,
    usage_settlement_snapshots.wallet_gift_balance_before
  ),
  wallet_gift_balance_after = COALESCE(
    excluded.wallet_gift_balance_after,
    usage_settlement_snapshots.wallet_gift_balance_after
  ),
  provider_monthly_used_usd = COALESCE(
    excluded.provider_monthly_used_usd,
    usage_settlement_snapshots.provider_monthly_used_usd
  ),
  finalized_at = COALESCE(excluded.finalized_at, usage_settlement_snapshots.finalized_at),
  updated_at = excluded.updated_at
"#;

const ENQUEUE_PROVIDER_MONTHLY_USAGE_DELTA_SQL: &str = r#"
INSERT INTO usage_counter_deltas (
  id, request_id, kind, target_id, total_cost_usd_delta, created_at
)
VALUES (?, ?, 'provider_monthly', ?, ?, ?)
"#;

#[derive(Debug, Clone)]
pub struct SqliteSettlementRepository {
    pool: SqlitePool,
}

impl SqliteSettlementRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn usage_policy_cost_i64(value: u64, field: &str) -> Result<i64, DataLayerError> {
    i64::try_from(value)
        .map_err(|_| DataLayerError::InvalidInput(format!("{field} exceeds the integer range")))
}

fn usage_policy_cost_u64(value: i64, field: &str) -> Result<u64, DataLayerError> {
    u64::try_from(value)
        .map_err(|_| DataLayerError::UnexpectedValue(format!("{field} must not be negative")))
}

fn usage_policy_request_admission_from_sqlite_row(
    row: &SqliteRow,
) -> Result<StoredUsagePolicyRequestAdmission, DataLayerError> {
    let state: String = row.try_get("state").map_sql_err()?;
    Ok(StoredUsagePolicyRequestAdmission {
        request_id: row.try_get("request_id").map_sql_err()?,
        subject_id: row.try_get("subject_id").map_sql_err()?,
        event_token: row.try_get("event_token").map_sql_err()?,
        admitted_at_unix_secs: usage_policy_cost_u64(
            row.try_get("admitted_at_unix_secs").map_sql_err()?,
            "usage policy request admitted_at",
        )?,
        retain_until_unix_secs: usage_policy_cost_u64(
            row.try_get("retain_until_unix_secs").map_sql_err()?,
            "usage policy request retain_until",
        )?,
        state: UsagePolicyRequestAdmissionState::parse(&state).ok_or_else(|| {
            DataLayerError::UnexpectedValue(format!(
                "unknown usage policy request admission state {state}"
            ))
        })?,
        released_at_unix_secs: row
            .try_get::<Option<i64>, _>("released_at_unix_secs")
            .map_sql_err()?
            .map(|value| usage_policy_cost_u64(value, "usage policy request released_at"))
            .transpose()?,
    })
}

const FIND_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL: &str = r#"
SELECT request_id, subject_id, event_token,
       admitted_at AS admitted_at_unix_secs,
       retain_until AS retain_until_unix_secs,
       state, released_at AS released_at_unix_secs
FROM usage_request_admissions
WHERE event_token = ?
"#;

const INSERT_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL: &str = r#"
INSERT INTO usage_request_admissions (
  request_id, subject_id, event_token, admitted_at, retain_until,
  state, released_at, created_at
) VALUES (?, ?, ?, ?, ?, 'active', NULL, ?)
ON CONFLICT(event_token) DO NOTHING
"#;

fn usage_policy_cost_reservation_from_sqlite_row(
    row: &SqliteRow,
) -> Result<StoredUsagePolicyCostReservation, DataLayerError> {
    let state: String = row.try_get("state").map_sql_err()?;
    Ok(StoredUsagePolicyCostReservation {
        request_id: row.try_get("request_id").map_sql_err()?,
        subject_id: row.try_get("subject_id").map_sql_err()?,
        reservation_token: row.try_get("reservation_token").map_sql_err()?,
        admitted_at_unix_secs: usage_policy_cost_u64(
            row.try_get("admitted_at").map_sql_err()?,
            "usage policy admitted_at",
        )?,
        reserved_cost_units: usage_policy_cost_u64(
            row.try_get("reserved_cost_units").map_sql_err()?,
            "usage policy reserved_cost_units",
        )?,
        actual_cost_units: row
            .try_get::<Option<i64>, _>("actual_cost_units")
            .map_sql_err()?
            .map(|value| usage_policy_cost_u64(value, "usage policy actual_cost_units"))
            .transpose()?,
        state: UsagePolicyCostReservationState::parse(&state).ok_or_else(|| {
            DataLayerError::UnexpectedValue(format!(
                "unknown usage policy reservation state {state}"
            ))
        })?,
        reservation_expires_at_unix_secs: usage_policy_cost_u64(
            row.try_get("reservation_expires_at").map_sql_err()?,
            "usage policy reservation_expires_at",
        )?,
        retain_until_unix_secs: usage_policy_cost_u64(
            row.try_get("retain_until").map_sql_err()?,
            "usage policy retain_until",
        )?,
        finalized_at_unix_secs: row
            .try_get::<Option<i64>, _>("finalized_at")
            .map_sql_err()?
            .map(|value| usage_policy_cost_u64(value, "usage policy finalized_at"))
            .transpose()?,
    })
}

const FIND_USAGE_POLICY_COST_RESERVATION_SQLITE_SQL: &str = r#"
SELECT request_id, subject_id, reservation_token, admitted_at,
       reserved_cost_units, actual_cost_units, state,
       reservation_expires_at, retain_until, finalized_at
FROM usage_cost_reservations
WHERE reservation_token = ?
"#;

async fn lock_usage_policy_subject_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    subject_id: &str,
) -> Result<(), DataLayerError> {
    let result = sqlx::query("UPDATE users SET updated_at = updated_at WHERE id = ?")
        .bind(subject_id)
        .execute(&mut **tx)
        .await
        .map_sql_err()?;
    if result.rows_affected() == 0 {
        return Err(DataLayerError::InvalidInput(
            "usage policy subject does not exist".to_string(),
        ));
    }
    Ok(())
}

fn settlement_from_row(row: &SqliteRow) -> Result<StoredUsageSettlement, DataLayerError> {
    Ok(StoredUsageSettlement {
        request_id: row.try_get("request_id").map_sql_err()?,
        wallet_id: row.try_get("wallet_id").map_sql_err()?,
        billing_status: row.try_get("billing_status").map_sql_err()?,
        wallet_balance_before: sqlite_optional_real(row, "wallet_balance_before")?,
        wallet_balance_after: sqlite_optional_real(row, "wallet_balance_after")?,
        wallet_recharge_balance_before: sqlite_optional_real(
            row,
            "wallet_recharge_balance_before",
        )?,
        wallet_recharge_balance_after: sqlite_optional_real(row, "wallet_recharge_balance_after")?,
        wallet_gift_balance_before: sqlite_optional_real(row, "wallet_gift_balance_before")?,
        wallet_gift_balance_after: sqlite_optional_real(row, "wallet_gift_balance_after")?,
        provider_monthly_used_usd: sqlite_optional_real(row, "provider_monthly_used_usd")?,
        finalized_at_unix_secs: row
            .try_get::<Option<i64>, _>("finalized_at_unix_secs")
            .map_sql_err()?
            .map(|value| value as u64),
    })
}

fn now_unix_secs() -> Result<i64, DataLayerError> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
    .map_err(|_| DataLayerError::InvalidInput("timestamp overflow".to_string()))
}

async fn enqueue_provider_monthly_usage_delta_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request_id: &str,
    provider_id: &str,
    total_cost_usd_delta: f64,
    created_at: i64,
) -> Result<(), DataLayerError> {
    let request_id = request_id.trim();
    let provider_id = provider_id.trim();
    if request_id.is_empty() || provider_id.is_empty() || total_cost_usd_delta == 0.0 {
        return Ok(());
    }
    if !total_cost_usd_delta.is_finite() {
        return Err(DataLayerError::UnexpectedValue(format!(
            "provider monthly usage delta is not finite for {provider_id}"
        )));
    }

    sqlx::query(ENQUEUE_PROVIDER_MONTHLY_USAGE_DELTA_SQL)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(request_id)
        .bind(provider_id)
        .bind(total_cost_usd_delta)
        .bind(created_at)
        .execute(&mut **tx)
        .await
        .map_sql_err()?;
    Ok(())
}

#[derive(Debug, Default)]
struct DailyQuotaDebitResult {
    debited_usd: f64,
    insufficient: bool,
}

#[derive(Debug)]
struct DailyQuotaGrant {
    entitlement_id: String,
    daily_quota_usd: f64,
    usage_date: String,
    allow_wallet_overage: bool,
}

fn daily_quota_usage_date(
    reset_timezone: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<String, DataLayerError> {
    let timezone = reset_timezone
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Asia/Shanghai")
        .parse::<chrono_tz::Tz>()
        .map_err(|err| DataLayerError::InvalidInput(format!("invalid reset_timezone: {err}")))?;
    Ok(now.with_timezone(&timezone).date_naive().to_string())
}

fn daily_quota_grants_from_entitlement(
    entitlement_id: &str,
    entitlements: &serde_json::Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<DailyQuotaGrant>, DataLayerError> {
    let mut grants = Vec::new();
    let Some(items) = entitlements.as_array() else {
        return Ok(grants);
    };
    for item in items {
        if item.get("type").and_then(serde_json::Value::as_str) != Some("daily_quota") {
            continue;
        }
        let daily_quota_usd = item
            .get("daily_quota_usd")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        if !daily_quota_usd.is_finite() || daily_quota_usd <= 0.0 {
            continue;
        }
        grants.push(DailyQuotaGrant {
            entitlement_id: entitlement_id.to_string(),
            daily_quota_usd,
            usage_date: daily_quota_usage_date(
                item.get("reset_timezone")
                    .and_then(serde_json::Value::as_str),
                now,
            )?,
            allow_wallet_overage: item
                .get("allow_wallet_overage")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        });
    }
    Ok(grants)
}

async fn consume_daily_quota_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    user_id: &str,
    request_id: &str,
    total_cost_usd: f64,
    wallet_available_usd: Option<f64>,
    wallet_can_overdraft: bool,
    now_unix_secs: i64,
) -> Result<DailyQuotaDebitResult, DataLayerError> {
    if total_cost_usd <= 0.0 {
        return Ok(DailyQuotaDebitResult::default());
    }
    let rows = sqlx::query(
        r#"
SELECT id, entitlements_snapshot
FROM user_plan_entitlements
WHERE user_id = ?
  AND status = 'active'
  AND starts_at <= ?
  AND expires_at > ?
ORDER BY expires_at ASC, created_at ASC, id ASC
"#,
    )
    .bind(user_id)
    .bind(now_unix_secs)
    .bind(now_unix_secs)
    .fetch_all(&mut **tx)
    .await
    .map_sql_err()?;
    let now = chrono::Utc::now();
    let mut grants = Vec::new();
    for row in rows {
        let entitlement_id: String = row.try_get("id").map_sql_err()?;
        let entitlements_raw: String = row.try_get("entitlements_snapshot").map_sql_err()?;
        let entitlements =
            serde_json::from_str::<serde_json::Value>(&entitlements_raw).map_err(|err| {
                DataLayerError::UnexpectedValue(format!(
                    "user_plan_entitlements.entitlements_snapshot invalid json: {err}"
                ))
            })?;
        grants.extend(daily_quota_grants_from_entitlement(
            &entitlement_id,
            &entitlements,
            now,
        )?);
    }
    if grants.is_empty() {
        return Ok(DailyQuotaDebitResult::default());
    }

    let mut grants_with_remaining = Vec::new();
    let mut total_remaining = 0.0;
    let mut allow_wallet_overage = true;
    for grant in grants {
        allow_wallet_overage &= grant.allow_wallet_overage;
        let used = sqlx::query_scalar::<_, f64>(
            r#"
SELECT CAST(COALESCE(SUM(amount_usd), 0) AS REAL)
FROM entitlement_usage_ledgers
WHERE user_entitlement_id = ?
  AND usage_date = ?
"#,
        )
        .bind(&grant.entitlement_id)
        .bind(&grant.usage_date)
        .fetch_one(&mut **tx)
        .await
        .map_sql_err()?;
        let remaining = (grant.daily_quota_usd - used).max(0.0);
        total_remaining += remaining;
        grants_with_remaining.push((grant, remaining));
    }
    if !allow_wallet_overage && total_remaining + 0.000_000_01 < total_cost_usd {
        return Ok(DailyQuotaDebitResult {
            debited_usd: 0.0,
            insufficient: true,
        });
    }
    if allow_wallet_overage
        && !wallet_can_overdraft
        && wallet_available_usd.is_some_and(|available| {
            total_remaining + available + SETTLEMENT_EPSILON_USD < total_cost_usd
        })
    {
        return Ok(DailyQuotaDebitResult {
            debited_usd: 0.0,
            insufficient: true,
        });
    }

    let mut remaining_cost = total_cost_usd;
    let mut debited = 0.0;
    for (grant, balance_before) in grants_with_remaining {
        if remaining_cost <= 0.000_000_01 || balance_before <= 0.0 {
            continue;
        }
        let amount = remaining_cost.min(balance_before);
        let balance_after = balance_before - amount;
        sqlx::query(
            r#"
INSERT OR IGNORE INTO entitlement_usage_ledgers (
  id, user_entitlement_id, user_id, request_id, amount_usd,
  balance_before, balance_after, usage_date, created_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&grant.entitlement_id)
        .bind(user_id)
        .bind(request_id)
        .bind(amount)
        .bind(balance_before)
        .bind(balance_after)
        .bind(&grant.usage_date)
        .bind(now_unix_secs)
        .execute(&mut **tx)
        .await
        .map_sql_err()?;
        remaining_cost -= amount;
        debited += amount;
    }
    Ok(DailyQuotaDebitResult {
        debited_usd: debited,
        insufficient: false,
    })
}

#[async_trait]
impl SettlementWriteRepository for SqliteSettlementRepository {
    async fn reserve_usage_policy_request(
        &self,
        input: ReserveUsagePolicyRequestInput,
    ) -> Result<ReserveUsagePolicyRequestOutcome, DataLayerError> {
        input.validate()?;
        let now = now_unix_secs()?;
        let mut tx = self.pool.begin().await.map_sql_err()?;
        // This no-op update acquires SQLite's single writer slot before any admission reads.
        lock_usage_policy_subject_sqlite(&mut tx, &input.subject_id).await?;
        let existing_row = sqlx::query(FIND_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL)
            .bind(&input.event_token)
            .fetch_optional(&mut *tx)
            .await
            .map_sql_err()?;
        if let Some(row) = existing_row.as_ref() {
            let existing = usage_policy_request_admission_from_sqlite_row(row)?;
            if existing.request_id != input.request_id || existing.subject_id != input.subject_id {
                tx.commit().await.map_sql_err()?;
                return Ok(ReserveUsagePolicyRequestOutcome::Conflict);
            }
            if existing.admitted_at_unix_secs != input.admitted_at_unix_secs {
                return Err(DataLayerError::InvalidInput(
                    "usage policy event_token must keep its original admitted_at".to_string(),
                ));
            }
            sqlx::query(
                "UPDATE usage_request_admissions SET retain_until = MAX(retain_until, ?) WHERE event_token = ?",
            )
            .bind(usage_policy_cost_i64(
                input.retain_until_unix_secs,
                "usage policy request retain_until",
            )?)
            .bind(&input.event_token)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
            let outcome = match existing.state {
                UsagePolicyRequestAdmissionState::Active => {
                    ReserveUsagePolicyRequestOutcome::Allowed
                }
                UsagePolicyRequestAdmissionState::Released => {
                    ReserveUsagePolicyRequestOutcome::AlreadyReleased
                }
            };
            tx.commit().await.map_sql_err()?;
            return Ok(outcome);
        }

        for (window_index, window) in input.windows.iter().enumerate() {
            let used_requests = sqlx::query_scalar::<_, i64>(
                r#"
SELECT COUNT(*)
FROM usage_request_admissions
WHERE subject_id = ?
  AND state = 'active'
  AND admitted_at >= ?
  AND admitted_at < ?
                "#,
            )
            .bind(&input.subject_id)
            .bind(usage_policy_cost_i64(
                window.starts_at_unix_secs,
                "usage policy request window start",
            )?)
            .bind(usage_policy_cost_i64(
                window.ends_at_unix_secs,
                "usage policy request window end",
            )?)
            .fetch_one(&mut *tx)
            .await
            .map_sql_err()?;
            let used_requests =
                usage_policy_cost_u64(used_requests, "usage policy request used_requests")?;
            if used_requests >= window.limit_requests {
                tx.commit().await.map_sql_err()?;
                return Ok(ReserveUsagePolicyRequestOutcome::Rejected {
                    window_index,
                    limit_requests: window.limit_requests,
                    used_requests,
                });
            }
        }

        let insert_result = sqlx::query(INSERT_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL)
            .bind(&input.request_id)
            .bind(&input.subject_id)
            .bind(&input.event_token)
            .bind(usage_policy_cost_i64(
                input.admitted_at_unix_secs,
                "usage policy request admitted_at",
            )?)
            .bind(usage_policy_cost_i64(
                input.retain_until_unix_secs,
                "usage policy request retain_until",
            )?)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
        if insert_result.rows_affected() == 1 {
            tx.commit().await.map_sql_err()?;
            return Ok(ReserveUsagePolicyRequestOutcome::Allowed);
        }

        // The writer lock above normally makes this branch unreachable for concurrent reserves,
        // but classify the unique-token race explicitly so future lock changes cannot surface a
        // raw SQLite constraint error or accidentally reactivate a released tombstone.
        let row = sqlx::query(FIND_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL)
            .bind(&input.event_token)
            .fetch_one(&mut *tx)
            .await
            .map_sql_err()?;
        let existing = usage_policy_request_admission_from_sqlite_row(&row)?;
        if existing.request_id != input.request_id || existing.subject_id != input.subject_id {
            tx.commit().await.map_sql_err()?;
            return Ok(ReserveUsagePolicyRequestOutcome::Conflict);
        }
        if existing.admitted_at_unix_secs != input.admitted_at_unix_secs {
            return Err(DataLayerError::InvalidInput(
                "usage policy event_token must keep its original admitted_at".to_string(),
            ));
        }
        sqlx::query(
            "UPDATE usage_request_admissions SET retain_until = MAX(retain_until, ?) WHERE event_token = ?",
        )
        .bind(usage_policy_cost_i64(
            input.retain_until_unix_secs,
            "usage policy request retain_until",
        )?)
        .bind(&input.event_token)
        .execute(&mut *tx)
        .await
        .map_sql_err()?;
        let outcome = match existing.state {
            UsagePolicyRequestAdmissionState::Active => ReserveUsagePolicyRequestOutcome::Allowed,
            UsagePolicyRequestAdmissionState::Released => {
                ReserveUsagePolicyRequestOutcome::AlreadyReleased
            }
        };
        tx.commit().await.map_sql_err()?;
        Ok(outcome)
    }

    async fn release_usage_policy_request_admission(
        &self,
        input: ReleaseUsagePolicyRequestAdmissionInput,
    ) -> Result<Option<StoredUsagePolicyRequestAdmission>, DataLayerError> {
        input.validate()?;
        let mut tx = self.pool.begin().await.map_sql_err()?;
        lock_usage_policy_subject_sqlite(&mut tx, &input.subject_id).await?;
        let row = sqlx::query(FIND_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL)
            .bind(&input.event_token)
            .fetch_optional(&mut *tx)
            .await
            .map_sql_err()?;
        let Some(row) = row else {
            tx.commit().await.map_sql_err()?;
            return Ok(None);
        };
        let mut admission = usage_policy_request_admission_from_sqlite_row(&row)?;
        if admission.request_id != input.request_id || admission.subject_id != input.subject_id {
            tx.commit().await.map_sql_err()?;
            return Ok(None);
        }
        if input.released_at_unix_secs < admission.admitted_at_unix_secs {
            return Err(DataLayerError::InvalidInput(
                "usage policy released_at must not precede admitted_at".to_string(),
            ));
        }
        if admission.state == UsagePolicyRequestAdmissionState::Active {
            sqlx::query(
                "UPDATE usage_request_admissions SET state = 'released', released_at = ? WHERE event_token = ? AND state = 'active'",
            )
            .bind(usage_policy_cost_i64(
                input.released_at_unix_secs,
                "usage policy request released_at",
            )?)
            .bind(&input.event_token)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
            admission.state = UsagePolicyRequestAdmissionState::Released;
            admission.released_at_unix_secs = Some(input.released_at_unix_secs);
        }
        tx.commit().await.map_sql_err()?;
        Ok(Some(admission))
    }

    async fn cleanup_usage_policy_request_admissions(
        &self,
        now_unix_secs: u64,
        batch_size: usize,
    ) -> Result<usize, DataLayerError> {
        if batch_size == 0 {
            return Ok(0);
        }
        let now = usage_policy_cost_i64(now_unix_secs, "usage policy request cleanup timestamp")?;
        let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let result = sqlx::query(
            r#"
DELETE FROM usage_request_admissions
WHERE rowid IN (
  SELECT rowid
  FROM usage_request_admissions
  WHERE retain_until <= ?
  ORDER BY retain_until, event_token
  LIMIT ?
)
            "#,
        )
        .bind(now)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_sql_err()?;
        Ok(result.rows_affected() as usize)
    }

    async fn reserve_usage_policy_cost(
        &self,
        input: ReserveUsagePolicyCostInput,
    ) -> Result<ReserveUsagePolicyCostOutcome, DataLayerError> {
        input.validate()?;
        let now = now_unix_secs()?;
        let mut tx = self.pool.begin().await.map_sql_err()?;
        lock_usage_policy_subject_sqlite(&mut tx, &input.subject_id).await?;
        let existing_row = sqlx::query(FIND_USAGE_POLICY_COST_RESERVATION_SQLITE_SQL)
            .bind(&input.reservation_token)
            .fetch_optional(&mut *tx)
            .await
            .map_sql_err()?;
        let existing = existing_row
            .as_ref()
            .map(usage_policy_cost_reservation_from_sqlite_row)
            .transpose()?;
        if let Some(existing) = existing.as_ref() {
            if existing.request_id != input.request_id || existing.subject_id != input.subject_id {
                tx.commit().await.map_sql_err()?;
                return Ok(ReserveUsagePolicyCostOutcome::Conflict);
            }
            if existing.state != UsagePolicyCostReservationState::Reserved {
                tx.commit().await.map_sql_err()?;
                return Ok(ReserveUsagePolicyCostOutcome::AlreadyTerminal {
                    state: existing.state,
                });
            }
            if existing.admitted_at_unix_secs != input.admitted_at_unix_secs {
                return Err(DataLayerError::InvalidInput(
                    "usage policy reservation_token must keep its original admitted_at".to_string(),
                ));
            }
        }

        let previous_reserved_cost_units = existing
            .as_ref()
            .map(|reservation| reservation.reserved_cost_units)
            .unwrap_or(0);
        let target_reserved_cost_units =
            previous_reserved_cost_units.max(input.reserved_cost_units);
        for (window_index, window) in input.windows.iter().enumerate() {
            let used_cost_units = sqlx::query_scalar::<_, i64>(
                r#"
SELECT COALESCE(SUM(
  CASE
    WHEN state = 'finalized' THEN COALESCE(actual_cost_units, 0)
    WHEN state = 'reserved' AND reservation_expires_at > ? THEN reserved_cost_units
    ELSE 0
  END
), 0)
FROM usage_cost_reservations
WHERE subject_id = ?
  AND admitted_at >= ?
  AND admitted_at < ?
  AND reservation_token <> ?
                "#,
            )
            .bind(usage_policy_cost_i64(
                input.admitted_at_unix_secs,
                "usage policy admitted_at",
            )?)
            .bind(&input.subject_id)
            .bind(usage_policy_cost_i64(
                window.starts_at_unix_secs,
                "usage policy window start",
            )?)
            .bind(usage_policy_cost_i64(
                window.ends_at_unix_secs,
                "usage policy window end",
            )?)
            .bind(&input.reservation_token)
            .fetch_one(&mut *tx)
            .await
            .map_sql_err()?;
            let used_cost_units =
                usage_policy_cost_u64(used_cost_units, "usage policy used_cost_units")?;
            if used_cost_units
                .checked_add(target_reserved_cost_units)
                .is_none_or(|total| total > window.limit_cost_units)
            {
                tx.commit().await.map_sql_err()?;
                return Ok(ReserveUsagePolicyCostOutcome::Rejected {
                    window_index,
                    limit_cost_units: window.limit_cost_units,
                    used_cost_units,
                });
            }
        }

        sqlx::query(
            r#"
INSERT INTO usage_cost_reservations (
  request_id, subject_id, reservation_token, admitted_at,
  reserved_cost_units, actual_cost_units,
  state, reservation_expires_at, retain_until, finalized_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, NULL, 'reserved', ?, ?, NULL, ?, ?)
ON CONFLICT (reservation_token) DO UPDATE SET
  reserved_cost_units = MAX(
    usage_cost_reservations.reserved_cost_units,
    excluded.reserved_cost_units
  ),
  reservation_expires_at = MAX(
    usage_cost_reservations.reservation_expires_at,
    excluded.reservation_expires_at
  ),
  retain_until = MAX(
    usage_cost_reservations.retain_until,
    excluded.retain_until
  ),
  updated_at = excluded.updated_at
            "#,
        )
        .bind(&input.request_id)
        .bind(&input.subject_id)
        .bind(&input.reservation_token)
        .bind(usage_policy_cost_i64(
            input.admitted_at_unix_secs,
            "usage policy admitted_at",
        )?)
        .bind(usage_policy_cost_i64(
            target_reserved_cost_units,
            "usage policy reserved_cost_units",
        )?)
        .bind(usage_policy_cost_i64(
            input.reservation_expires_at_unix_secs,
            "usage policy reservation_expires_at",
        )?)
        .bind(usage_policy_cost_i64(
            input.retain_until_unix_secs,
            "usage policy retain_until",
        )?)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_sql_err()?;
        tx.commit().await.map_sql_err()?;
        Ok(ReserveUsagePolicyCostOutcome::Allowed {
            reserved_cost_units: target_reserved_cost_units,
            additional_reserved_cost_units: target_reserved_cost_units
                .saturating_sub(previous_reserved_cost_units),
        })
    }

    async fn reconcile_usage_policy_cost(
        &self,
        input: ReconcileUsagePolicyCostInput,
    ) -> Result<Option<StoredUsagePolicyCostReservation>, DataLayerError> {
        input.validate()?;
        let now = now_unix_secs()?;
        let mut tx = self.pool.begin().await.map_sql_err()?;
        lock_usage_policy_subject_sqlite(&mut tx, &input.subject_id).await?;
        let row = sqlx::query(FIND_USAGE_POLICY_COST_RESERVATION_SQLITE_SQL)
            .bind(&input.reservation_token)
            .fetch_optional(&mut *tx)
            .await
            .map_sql_err()?;
        let Some(row) = row else {
            tx.commit().await.map_sql_err()?;
            return Ok(None);
        };
        let mut reservation = usage_policy_cost_reservation_from_sqlite_row(&row)?;
        if reservation.request_id != input.request_id || reservation.subject_id != input.subject_id
        {
            // The token selects the row; audit identity must still match before the reservation
            // can be finalized.
            tx.commit().await.map_sql_err()?;
            return Ok(None);
        }
        if reservation.state == UsagePolicyCostReservationState::Reserved {
            sqlx::query(
                r#"
UPDATE usage_cost_reservations
SET state = ?, actual_cost_units = ?, finalized_at = ?, updated_at = ?
WHERE reservation_token = ?
  AND request_id = ?
  AND subject_id = ?
  AND state = 'reserved'
                "#,
            )
            .bind(input.terminal_state.as_str())
            .bind(usage_policy_cost_i64(
                input.actual_cost_units,
                "usage policy actual_cost_units",
            )?)
            .bind(usage_policy_cost_i64(
                input.finalized_at_unix_secs,
                "usage policy finalized_at",
            )?)
            .bind(now)
            .bind(&input.reservation_token)
            .bind(&input.request_id)
            .bind(&input.subject_id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
            reservation.state = input.terminal_state;
            reservation.actual_cost_units = Some(input.actual_cost_units);
            reservation.finalized_at_unix_secs = Some(input.finalized_at_unix_secs);
        }
        tx.commit().await.map_sql_err()?;
        Ok(Some(reservation))
    }

    async fn cleanup_usage_policy_cost_reservations(
        &self,
        now_unix_secs: u64,
        batch_size: usize,
    ) -> Result<usize, DataLayerError> {
        if batch_size == 0 {
            return Ok(0);
        }
        let now = usage_policy_cost_i64(now_unix_secs, "usage policy cleanup timestamp")?;
        let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let result = sqlx::query(
            r#"
DELETE FROM usage_cost_reservations
WHERE rowid IN (
  SELECT rowid
  FROM usage_cost_reservations
  WHERE retain_until <= ?
  ORDER BY retain_until, reservation_token
  LIMIT ?
)
            "#,
        )
        .bind(now)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_sql_err()?;
        Ok(result.rows_affected() as usize)
    }

    async fn settle_usage(
        &self,
        input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        input.validate()?;
        let finalized_at = i64::try_from(
            input
                .finalized_at_unix_secs
                .unwrap_or(now_unix_secs()? as u64),
        )
        .map_err(|_| DataLayerError::InvalidInput("finalized_at overflow".to_string()))?;
        let updated_at = now_unix_secs()?;

        let mut tx = self.pool.begin().await.map_sql_err()?;
        // SQLite transactions are deferred. Acquire the single writer slot before reading the
        // billing status so concurrent settlement attempts cannot both observe `pending`.
        sqlx::query("UPDATE \"usage\" SET billing_status = billing_status WHERE 0")
            .execute(&mut *tx)
            .await
            .map_sql_err()?;
        let row = sqlx::query(FIND_USAGE_FOR_SETTLEMENT_SQL)
            .bind(&input.request_id)
            .fetch_optional(&mut *tx)
            .await
            .map_sql_err()?;

        let Some(usage_row) = row else {
            tx.commit().await.map_sql_err()?;
            return Ok(None);
        };

        let current_billing_status: String = usage_row.try_get("billing_status").map_sql_err()?;
        if matches!(
            current_billing_status.as_str(),
            "settled" | "void" | "insufficient_quota"
        ) {
            let settlement = settlement_from_row(&usage_row)?;
            tx.commit().await.map_sql_err()?;
            return Ok(Some(settlement));
        }

        let mut final_billing_status =
            settlement_billing_status_for_usage_status(&input.status).to_string();
        let mut settlement = StoredUsageSettlement {
            request_id: input.request_id.clone(),
            wallet_id: None,
            billing_status: final_billing_status.clone(),
            wallet_balance_before: None,
            wallet_balance_after: None,
            wallet_recharge_balance_before: None,
            wallet_recharge_balance_after: None,
            wallet_gift_balance_before: None,
            wallet_gift_balance_after: None,
            provider_monthly_used_usd: None,
            finalized_at_unix_secs: Some(finalized_at as u64),
        };

        if final_billing_status == "settled" {
            let api_key_id = input
                .api_key_id
                .as_deref()
                .filter(|value| !value.is_empty());
            let api_key_is_standalone = if input.api_key_is_standalone {
                true
            } else if let Some(api_key_id) = api_key_id {
                sqlx::query_scalar::<_, bool>(
                    r#"
SELECT is_standalone
FROM api_keys
WHERE id = ?
LIMIT 1
"#,
                )
                .bind(api_key_id)
                .fetch_optional(&mut *tx)
                .await
                .map_sql_err()?
                .unwrap_or(false)
            } else {
                false
            };

            let wallet_row = if let Some(api_key_id) = api_key_id {
                sqlx::query(
                    r#"
SELECT id, balance, gift_balance, limit_mode
FROM wallets
WHERE api_key_id = ?
LIMIT 1
"#,
                )
                .bind(api_key_id)
                .fetch_optional(&mut *tx)
                .await
                .map_sql_err()?
            } else {
                None
            };

            let wallet_row = if wallet_row.is_some() {
                wallet_row
            } else if !api_key_is_standalone {
                if let Some(user_id) = input.user_id.as_deref().filter(|value| !value.is_empty()) {
                    sqlx::query(
                        r#"
SELECT id, balance, gift_balance, limit_mode
FROM wallets
WHERE user_id = ?
LIMIT 1
"#,
                    )
                    .bind(user_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_sql_err()?
                } else {
                    None
                }
            } else {
                None
            };

            let wallet_can_overdraft = wallet_row.is_some();
            let wallet_available_usd = match wallet_row.as_ref() {
                Some(row) => {
                    let limit_mode: String = row.try_get("limit_mode").map_sql_err()?;
                    if limit_mode.eq_ignore_ascii_case("unlimited") {
                        None
                    } else {
                        Some(finite_wallet_available_usd(
                            sqlite_real(row, "balance")?,
                            sqlite_real(row, "gift_balance")?,
                        ))
                    }
                }
                None => Some(0.0),
            };
            if let Some(row) = wallet_row.as_ref() {
                let wallet_id: String = row.try_get("id").map_sql_err()?;
                let before_recharge = sqlite_real(row, "balance")?;
                let before_gift = sqlite_real(row, "gift_balance")?;
                let before_total = before_recharge + before_gift;
                settlement.wallet_id = Some(wallet_id);
                settlement.wallet_balance_before = Some(before_total);
                settlement.wallet_balance_after = Some(before_total);
                settlement.wallet_recharge_balance_before = Some(before_recharge);
                settlement.wallet_recharge_balance_after = Some(before_recharge);
                settlement.wallet_gift_balance_before = Some(before_gift);
                settlement.wallet_gift_balance_after = Some(before_gift);
            }

            let billable_cost_usd = settlement_billable_cost_usd(&input);
            let wallet_debit_cost_usd = if !api_key_is_standalone {
                if let Some(user_id) = input.user_id.as_deref().filter(|value| !value.is_empty()) {
                    let quota = consume_daily_quota_sqlite(
                        &mut tx,
                        user_id,
                        &input.request_id,
                        billable_cost_usd,
                        wallet_available_usd,
                        wallet_can_overdraft,
                        updated_at,
                    )
                    .await?;
                    if quota.insufficient {
                        final_billing_status = "insufficient_quota".to_string();
                        settlement.billing_status = final_billing_status.clone();
                        0.0
                    } else {
                        (billable_cost_usd - quota.debited_usd).max(0.0)
                    }
                } else {
                    billable_cost_usd
                }
            } else {
                billable_cost_usd
            };
            if final_billing_status != "settled" {
                sqlx::query(UPSERT_USAGE_SETTLEMENT_SNAPSHOT_SQL)
                    .bind(&settlement.request_id)
                    .bind(&settlement.billing_status)
                    .bind(settlement.wallet_id.as_deref())
                    .bind(settlement.wallet_balance_before)
                    .bind(settlement.wallet_balance_after)
                    .bind(settlement.wallet_recharge_balance_before)
                    .bind(settlement.wallet_recharge_balance_after)
                    .bind(settlement.wallet_gift_balance_before)
                    .bind(settlement.wallet_gift_balance_after)
                    .bind(settlement.provider_monthly_used_usd)
                    .bind(settlement.finalized_at_unix_secs.map(|value| value as i64))
                    .bind(updated_at)
                    .bind(updated_at)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                sqlx::query(FINALIZE_USAGE_BILLING_SQL)
                    .bind(&final_billing_status)
                    .bind(finalized_at)
                    .bind(&input.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                tx.commit().await.map_sql_err()?;
                return Ok(Some(settlement));
            }

            if wallet_debit_cost_usd > SETTLEMENT_EPSILON_USD {
                if let Some(wallet_row) = wallet_row {
                    let wallet_id: String = wallet_row.try_get("id").map_sql_err()?;
                    let before_recharge = sqlite_real(&wallet_row, "balance")?;
                    let before_gift = sqlite_real(&wallet_row, "gift_balance")?;
                    let limit_mode: String = wallet_row.try_get("limit_mode").map_sql_err()?;
                    let before_total = before_recharge + before_gift;
                    let mut after_recharge = before_recharge;
                    let mut after_gift = before_gift;
                    if !limit_mode.eq_ignore_ascii_case("unlimited") {
                        let debit_plan = plan_finite_wallet_debit(
                            before_recharge,
                            before_gift,
                            wallet_debit_cost_usd,
                        );
                        (after_recharge, after_gift) =
                            debit_plan.after_balances(before_recharge, before_gift);
                    }
                    if final_billing_status == "settled" {
                        sqlx::query(
                            r#"
UPDATE wallets
SET
  balance = ?,
  gift_balance = ?,
  total_consumed = COALESCE(total_consumed, 0) + ?,
  updated_at = ?
WHERE id = ?
"#,
                        )
                        .bind(after_recharge)
                        .bind(after_gift)
                        .bind(wallet_debit_cost_usd)
                        .bind(updated_at)
                        .bind(&wallet_id)
                        .execute(&mut *tx)
                        .await
                        .map_sql_err()?;
                    }

                    settlement.wallet_id = Some(wallet_id);
                    settlement.wallet_balance_before = Some(before_total);
                    settlement.wallet_balance_after = Some(after_recharge + after_gift);
                    settlement.wallet_recharge_balance_before = Some(before_recharge);
                    settlement.wallet_recharge_balance_after = Some(after_recharge);
                    settlement.wallet_gift_balance_before = Some(before_gift);
                    settlement.wallet_gift_balance_after = Some(after_gift);
                } else {
                    final_billing_status = "insufficient_quota".to_string();
                    settlement.billing_status = final_billing_status.clone();
                }
            }

            if final_billing_status != "settled" {
                sqlx::query(UPSERT_USAGE_SETTLEMENT_SNAPSHOT_SQL)
                    .bind(&settlement.request_id)
                    .bind(&settlement.billing_status)
                    .bind(settlement.wallet_id.as_deref())
                    .bind(settlement.wallet_balance_before)
                    .bind(settlement.wallet_balance_after)
                    .bind(settlement.wallet_recharge_balance_before)
                    .bind(settlement.wallet_recharge_balance_after)
                    .bind(settlement.wallet_gift_balance_before)
                    .bind(settlement.wallet_gift_balance_after)
                    .bind(settlement.provider_monthly_used_usd)
                    .bind(settlement.finalized_at_unix_secs.map(|value| value as i64))
                    .bind(updated_at)
                    .bind(updated_at)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                sqlx::query(FINALIZE_USAGE_BILLING_SQL)
                    .bind(&final_billing_status)
                    .bind(finalized_at)
                    .bind(&input.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                tx.commit().await.map_sql_err()?;
                return Ok(Some(settlement));
            }

            if let Some(provider_id) = input
                .provider_id
                .as_deref()
                .filter(|value| !value.is_empty())
            {
                enqueue_provider_monthly_usage_delta_sqlite(
                    &mut tx,
                    &input.request_id,
                    provider_id,
                    input.actual_total_cost_usd,
                    updated_at,
                )
                .await?;
            }
        }

        sqlx::query(UPSERT_USAGE_SETTLEMENT_SNAPSHOT_SQL)
            .bind(&settlement.request_id)
            .bind(&settlement.billing_status)
            .bind(settlement.wallet_id.as_deref())
            .bind(settlement.wallet_balance_before)
            .bind(settlement.wallet_balance_after)
            .bind(settlement.wallet_recharge_balance_before)
            .bind(settlement.wallet_recharge_balance_after)
            .bind(settlement.wallet_gift_balance_before)
            .bind(settlement.wallet_gift_balance_after)
            .bind(settlement.provider_monthly_used_usd)
            .bind(settlement.finalized_at_unix_secs.map(|value| value as i64))
            .bind(updated_at)
            .bind(updated_at)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;

        sqlx::query(FINALIZE_USAGE_BILLING_SQL)
            .bind(&final_billing_status)
            .bind(finalized_at)
            .bind(&input.request_id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;

        tx.commit().await.map_sql_err()?;
        Ok(Some(settlement))
    }
}

#[cfg(test)]
mod tests {
    use super::{SqliteSettlementRepository, INSERT_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL};
    use crate::run_migrations;
    use aether_data_contracts::repository::settlement::{
        SettlementWriteRepository, UsageSettlementInput,
    };
    use sqlx::Row;
    use std::time::Duration;

    #[tokio::test]
    async fn sqlite_repository_settles_usage_once() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-1".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 6.0,
                finalized_at_unix_secs: Some(1_234),
            })
            .await
            .expect("settlement should run")
            .expect("usage should exist");

        assert_eq!(settlement.billing_status, "settled");
        assert_eq!(settlement.wallet_id.as_deref(), Some("wallet-1"));
        assert_eq!(settlement.wallet_balance_before, Some(12.0));
        assert_eq!(settlement.wallet_balance_after, Some(6.0));
        assert_eq!(settlement.wallet_recharge_balance_after, Some(4.0));
        assert_eq!(settlement.wallet_gift_balance_after, Some(2.0));
        assert_eq!(settlement.provider_monthly_used_usd, None);

        let wallet = sqlx::query(
            "SELECT balance, gift_balance, total_consumed FROM wallets WHERE id = 'wallet-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("wallet should load");
        assert_eq!(wallet.try_get::<f64, _>("balance").unwrap(), 4.0);
        assert_eq!(wallet.try_get::<f64, _>("gift_balance").unwrap(), 2.0);
        assert_eq!(wallet.try_get::<f64, _>("total_consumed").unwrap(), 6.0);

        let second = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-1".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 6.0,
                finalized_at_unix_secs: Some(9_999),
            })
            .await
            .expect("second settlement should run")
            .expect("usage should exist");
        assert_eq!(second.finalized_at_unix_secs, Some(1_234));

        let provider_used: f64 =
            sqlx::query_scalar("SELECT monthly_used_usd FROM providers WHERE id = 'provider-1'")
                .fetch_one(&pool)
                .await
                .expect("provider should load");
        assert_eq!(provider_used, 5.0);
        let provider_delta: (i64, f64) = sqlx::query_as(
            r#"
SELECT COUNT(*), CAST(COALESCE(SUM(total_cost_usd_delta), 0) AS REAL)
FROM usage_counter_deltas
WHERE request_id = 'request-1'
  AND kind = 'provider_monthly'
  AND target_id = 'provider-1'
"#,
        )
        .fetch_one(&pool)
        .await
        .expect("provider delta should load");
        assert_eq!(provider_delta, (1, 6.0));

        let snapshot: (String, Option<String>, Option<f64>, Option<i64>) = sqlx::query_as(
            r#"
SELECT billing_status, wallet_id, wallet_balance_after, finalized_at
FROM usage_settlement_snapshots
WHERE request_id = 'request-1'
"#,
        )
        .fetch_one(&pool)
        .await
        .expect("canonical settlement snapshot should load");
        assert_eq!(
            snapshot,
            (
                "settled".to_string(),
                Some("wallet-1".to_string()),
                Some(6.0),
                Some(1_234),
            )
        );
    }

    #[tokio::test]
    async fn request_admission_insert_defensively_preserves_the_existing_token() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_migrations(&pool)
            .await
            .expect("sqlite migrations should run");

        let insert = |request_id: &'static str, subject_id: &'static str| {
            sqlx::query(INSERT_USAGE_POLICY_REQUEST_ADMISSION_SQLITE_SQL)
                .bind(request_id)
                .bind(subject_id)
                .bind("defensive-event-token")
                .bind(100_i64)
                .bind(200_i64)
                .bind(100_i64)
        };
        assert_eq!(
            insert("defensive-request-1", "defensive-user-1")
                .execute(&pool)
                .await
                .expect("initial admission should insert")
                .rows_affected(),
            1
        );
        assert_eq!(
            insert("defensive-request-2", "defensive-user-2")
                .execute(&pool)
                .await
                .expect("duplicate token should be ignored")
                .rows_affected(),
            0
        );
        let stored: (String, String, i64) = sqlx::query_as(
            "SELECT request_id, subject_id, admitted_at FROM usage_request_admissions WHERE event_token = 'defensive-event-token'",
        )
        .fetch_one(&pool)
        .await
        .expect("original admission should remain");
        assert_eq!(
            stored,
            (
                "defensive-request-1".to_string(),
                "defensive-user-1".to_string(),
                100,
            )
        );
    }

    #[tokio::test]
    async fn sqlite_repository_voids_failed_usage_without_wallet_mutation() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-2".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "failed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 2.0,
                finalized_at_unix_secs: Some(1_235),
            })
            .await
            .expect("settlement should run")
            .expect("usage should exist");

        assert_eq!(settlement.billing_status, "void");
        assert_eq!(settlement.wallet_id, None);
        let wallet_total: f64 =
            sqlx::query_scalar("SELECT balance + gift_balance FROM wallets WHERE id = 'wallet-1'")
                .fetch_one(&pool)
                .await
                .expect("wallet should load");
        assert_eq!(wallet_total, 12.0);
    }

    #[tokio::test]
    async fn sqlite_repository_overdraws_finite_wallet_and_settles_usage() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-overdraw".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 15.0,
                actual_total_cost_usd: 15.0,
                finalized_at_unix_secs: Some(1_236),
            })
            .await
            .expect("settlement should run")
            .expect("usage should exist");

        assert_eq!(settlement.billing_status, "settled");
        assert_eq!(settlement.wallet_id.as_deref(), Some("wallet-1"));
        assert_eq!(settlement.wallet_balance_before, Some(12.0));
        assert_eq!(settlement.wallet_balance_after, Some(-3.0));
        assert_eq!(settlement.wallet_recharge_balance_after, Some(-3.0));
        assert_eq!(settlement.wallet_gift_balance_after, Some(0.0));
        assert_eq!(settlement.provider_monthly_used_usd, None);

        let wallet = sqlx::query(
            "SELECT balance, gift_balance, total_consumed FROM wallets WHERE id = 'wallet-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("wallet should load");
        assert_eq!(wallet.try_get::<f64, _>("balance").unwrap(), -3.0);
        assert_eq!(wallet.try_get::<f64, _>("gift_balance").unwrap(), 0.0);
        assert_eq!(wallet.try_get::<f64, _>("total_consumed").unwrap(), 15.0);
        let provider_delta: f64 = sqlx::query_scalar(
            "SELECT total_cost_usd_delta FROM usage_counter_deltas WHERE request_id = 'request-overdraw' AND kind = 'provider_monthly'",
        )
        .fetch_one(&pool)
        .await
        .expect("provider delta should load");
        assert_eq!(provider_delta, 15.0);
    }

    #[tokio::test]
    async fn sqlite_repository_records_wallet_for_quota_covered_user_usage() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_quota_covered_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-quota-covered".to_string(),
                user_id: Some("user-quota".to_string()),
                api_key_id: Some("key-quota".to_string()),
                api_key_is_standalone: false,
                provider_id: None,
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 6.0,
                finalized_at_unix_secs: Some(1_260),
            })
            .await
            .expect("settlement should run")
            .expect("usage should exist");

        assert_eq!(settlement.billing_status, "settled");
        assert_eq!(settlement.wallet_id.as_deref(), Some("wallet-quota"));
        assert_eq!(settlement.wallet_balance_before, Some(0.0));
        assert_eq!(settlement.wallet_balance_after, Some(0.0));

        let wallet_total: f64 = sqlx::query_scalar(
            "SELECT balance + gift_balance FROM wallets WHERE id = 'wallet-quota'",
        )
        .fetch_one(&pool)
        .await
        .expect("wallet should load");
        assert_eq!(wallet_total, 0.0);

        let quota_used: f64 = sqlx::query_scalar(
            "SELECT CAST(COALESCE(SUM(amount_usd), 0) AS REAL) FROM entitlement_usage_ledgers WHERE request_id = 'request-quota-covered'",
        )
        .fetch_one(&pool)
        .await
        .expect("quota ledger should load");
        assert_eq!(quota_used, 6.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_repository_serializes_concurrent_settlement_attempts() {
        let database_path = std::env::temp_dir().join(format!(
            "aether-settlement-parity-{}.db",
            uuid::Uuid::new_v4()
        ));
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&database_path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .expect("sqlite pool should connect");
        run_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let input = UsageSettlementInput {
            request_id: "request-1".to_string(),
            user_id: Some("user-1".to_string()),
            api_key_id: None,
            api_key_is_standalone: false,
            provider_id: Some("provider-1".to_string()),
            status: "completed".to_string(),
            billing_status: "pending".to_string(),
            total_cost_usd: 3.0,
            actual_total_cost_usd: 6.0,
            finalized_at_unix_secs: Some(1_234),
        };
        let (first, second) = tokio::join!(
            repository.settle_usage(input.clone()),
            repository.settle_usage(input)
        );
        let first = first
            .expect("first settlement should succeed")
            .expect("usage should exist");
        let second = second
            .expect("second settlement should succeed")
            .expect("usage should exist");
        assert_eq!(first.billing_status, "settled");
        assert_eq!(second.billing_status, "settled");

        let wallet: (f64, f64, f64) = sqlx::query_as(
            "SELECT balance, gift_balance, total_consumed FROM wallets WHERE id = 'wallet-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("wallet should load");
        assert_eq!(wallet, (4.0, 2.0, 6.0));
        let delta_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM usage_counter_deltas WHERE request_id = 'request-1' AND kind = 'provider_monthly'",
        )
        .fetch_one(&pool)
        .await
        .expect("provider deltas should count");
        assert_eq!(delta_count, 1);

        pool.close().await;
        let _ = std::fs::remove_file(&database_path);
        let _ = std::fs::remove_file(format!("{}-wal", database_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", database_path.display()));
    }

    async fn seed_settlement_rows(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO providers (
  id, name, provider_type, monthly_used_usd, created_at, updated_at
)
VALUES ('provider-1', 'Provider One', 'openai', 5.0, 1, 1);

INSERT INTO wallets (
  id, user_id, balance, gift_balance, limit_mode, created_at, updated_at
)
VALUES ('wallet-1', 'user-1', 10.0, 2.0, 'finite', 1, 1);

INSERT INTO "usage" (
  request_id, user_id, provider_id, status, billing_status, total_cost_usd, actual_total_cost_usd
)
VALUES
  ('request-1', 'user-1', 'provider-1', 'completed', 'pending', 3.0, 6.0),
  ('request-2', 'user-1', 'provider-1', 'failed', 'pending', 3.0, 2.0),
  ('request-overdraw', 'user-1', 'provider-1', 'completed', 'pending', 15.0, 15.0);
"#,
        )
        .execute(pool)
        .await
        .expect("settlement rows should seed");
    }

    async fn seed_quota_covered_settlement_rows(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO users (
  id, username, email, role, auth_source, password_hash, is_active,
  is_deleted, created_at, updated_at
) VALUES (
  'user-quota', 'quota-user', 'quota@example.com', 'user', 'local',
  'hash', 1, 0, 1, 1
);

INSERT INTO wallets (
  id, user_id, balance, gift_balance, limit_mode, created_at, updated_at
) VALUES (
  'wallet-quota', 'user-quota', 0.0, 0.0, 'finite', 1, 1
);

INSERT INTO "usage" (
  request_id, user_id, api_key_id, status, billing_status,
  total_cost_usd, actual_total_cost_usd
) VALUES (
  'request-quota-covered', 'user-quota', 'key-quota', 'completed',
  'pending', 3.0, 6.0
);

INSERT INTO billing_plans (
  id, title, price_amount, price_currency, duration_unit,
  duration_value, entitlements_json, created_at, updated_at
) VALUES (
  'plan-quota', 'Quota Plan', 0.0, 'USD', 'month', 1,
  '[{"type":"daily_quota","daily_quota_usd":10.0,"reset_timezone":"Asia/Shanghai","allow_wallet_overage":false}]',
  1, 1
);

INSERT INTO payment_orders (
  id, order_no, wallet_id, user_id, amount_usd, refunded_amount_usd,
  refundable_amount_usd, payment_method, gateway_response, status, created_at
) VALUES (
  'order-quota', 'order-quota', 'wallet-quota', 'user-quota', 0.0, 0.0,
  0.0, 'admin_manual', '{}', 'credited', 1
);

INSERT INTO user_plan_entitlements (
  id, user_id, plan_id, payment_order_id, status, starts_at, expires_at,
  entitlements_snapshot, created_at, updated_at
) VALUES (
  'entitlement-quota', 'user-quota', 'plan-quota', 'order-quota',
  'active', 1, 9999999999,
  '[{"type":"daily_quota","daily_quota_usd":10.0,"reset_timezone":"Asia/Shanghai","allow_wallet_overage":false}]',
  1, 1
);
"#,
        )
        .execute(pool)
        .await
        .expect("quota settlement rows should seed");
    }
}
