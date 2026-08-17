CREATE TABLE IF NOT EXISTS usage_cost_reservations (
    request_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    reservation_token TEXT PRIMARY KEY NOT NULL,
    admitted_at INTEGER NOT NULL,
    reserved_cost_units INTEGER NOT NULL CHECK (reserved_cost_units >= 0),
    actual_cost_units INTEGER CHECK (actual_cost_units IS NULL OR actual_cost_units >= 0),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'finalized', 'released')),
    reservation_expires_at INTEGER NOT NULL,
    retain_until INTEGER NOT NULL,
    finalized_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (reservation_expires_at > admitted_at),
    CHECK (retain_until >= reservation_expires_at),
    CHECK (
        (state = 'reserved' AND actual_cost_units IS NULL AND finalized_at IS NULL)
        OR (state = 'finalized' AND actual_cost_units IS NOT NULL AND finalized_at IS NOT NULL)
        OR (state = 'released' AND actual_cost_units IS NOT NULL
            AND actual_cost_units = 0 AND finalized_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS usage_cost_reservations_request_id_idx
    ON usage_cost_reservations (request_id);

CREATE INDEX IF NOT EXISTS usage_cost_reservations_subject_admitted_at_idx
    ON usage_cost_reservations (subject_id, admitted_at);

CREATE INDEX IF NOT EXISTS usage_cost_reservations_reservation_expires_at_idx
    ON usage_cost_reservations (reservation_expires_at);

CREATE INDEX IF NOT EXISTS usage_cost_reservations_retain_until_token_idx
    ON usage_cost_reservations (retain_until, reservation_token);
