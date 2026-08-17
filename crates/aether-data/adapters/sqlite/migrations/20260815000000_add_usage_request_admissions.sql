CREATE TABLE IF NOT EXISTS usage_request_admissions (
    request_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    event_token TEXT PRIMARY KEY NOT NULL,
    admitted_at INTEGER NOT NULL,
    retain_until INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    released_at INTEGER,
    created_at INTEGER NOT NULL,
    CHECK (retain_until > admitted_at),
    CHECK (
        (state = 'active' AND released_at IS NULL)
        OR (state = 'released' AND released_at IS NOT NULL AND released_at >= admitted_at)
    )
);

CREATE INDEX IF NOT EXISTS usage_request_admissions_subject_admitted_at_idx
    ON usage_request_admissions (subject_id, admitted_at);

CREATE INDEX IF NOT EXISTS usage_request_admissions_retain_until_token_idx
    ON usage_request_admissions (retain_until, event_token);
