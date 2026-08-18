-- SQLite cannot add a foreign key with ALTER TABLE. Rebuild both ledgers while
-- preserving valid rows and dropping only records whose owning user was
-- already deleted before the relationship became enforceable.
CREATE TABLE usage_cost_reservations_with_user_fk (
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
    CONSTRAINT usage_cost_reservations_subject_id_fkey
        FOREIGN KEY (subject_id) REFERENCES users (id) ON DELETE CASCADE,
    CHECK (reservation_expires_at > admitted_at),
    CHECK (retain_until >= reservation_expires_at),
    CHECK (
        (state = 'reserved' AND actual_cost_units IS NULL AND finalized_at IS NULL)
        OR (state = 'finalized' AND actual_cost_units IS NOT NULL AND finalized_at IS NOT NULL)
        OR (state = 'released' AND actual_cost_units IS NOT NULL
            AND actual_cost_units = 0 AND finalized_at IS NOT NULL)
    )
);

INSERT INTO usage_cost_reservations_with_user_fk (
    request_id,
    subject_id,
    reservation_token,
    admitted_at,
    reserved_cost_units,
    actual_cost_units,
    state,
    reservation_expires_at,
    retain_until,
    finalized_at,
    created_at,
    updated_at
)
SELECT
    reservation.request_id,
    reservation.subject_id,
    reservation.reservation_token,
    reservation.admitted_at,
    reservation.reserved_cost_units,
    reservation.actual_cost_units,
    reservation.state,
    reservation.reservation_expires_at,
    reservation.retain_until,
    reservation.finalized_at,
    reservation.created_at,
    reservation.updated_at
FROM usage_cost_reservations AS reservation
INNER JOIN users AS app_user ON app_user.id = reservation.subject_id;

DROP TABLE usage_cost_reservations;
ALTER TABLE usage_cost_reservations_with_user_fk RENAME TO usage_cost_reservations;

CREATE INDEX usage_cost_reservations_request_id_idx
    ON usage_cost_reservations (request_id);
CREATE INDEX usage_cost_reservations_subject_admitted_at_idx
    ON usage_cost_reservations (subject_id, admitted_at);
CREATE INDEX usage_cost_reservations_reservation_expires_at_idx
    ON usage_cost_reservations (reservation_expires_at);
CREATE INDEX usage_cost_reservations_retain_until_token_idx
    ON usage_cost_reservations (retain_until, reservation_token);

CREATE TABLE usage_request_admissions_with_user_fk (
    request_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    event_token TEXT PRIMARY KEY NOT NULL,
    admitted_at INTEGER NOT NULL,
    retain_until INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    released_at INTEGER,
    created_at INTEGER NOT NULL,
    CONSTRAINT usage_request_admissions_subject_id_fkey
        FOREIGN KEY (subject_id) REFERENCES users (id) ON DELETE CASCADE,
    CHECK (retain_until > admitted_at),
    CHECK (
        (state = 'active' AND released_at IS NULL)
        OR (state = 'released' AND released_at IS NOT NULL AND released_at >= admitted_at)
    )
);

INSERT INTO usage_request_admissions_with_user_fk (
    request_id,
    subject_id,
    event_token,
    admitted_at,
    retain_until,
    state,
    released_at,
    created_at
)
SELECT
    admission.request_id,
    admission.subject_id,
    admission.event_token,
    admission.admitted_at,
    admission.retain_until,
    admission.state,
    admission.released_at,
    admission.created_at
FROM usage_request_admissions AS admission
INNER JOIN users AS app_user ON app_user.id = admission.subject_id;

DROP TABLE usage_request_admissions;
ALTER TABLE usage_request_admissions_with_user_fk RENAME TO usage_request_admissions;

CREATE INDEX usage_request_admissions_subject_admitted_at_idx
    ON usage_request_admissions (subject_id, admitted_at);
CREATE INDEX usage_request_admissions_retain_until_token_idx
    ON usage_request_admissions (retain_until, event_token);
