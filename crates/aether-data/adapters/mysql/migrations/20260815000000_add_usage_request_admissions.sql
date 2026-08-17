CREATE TABLE IF NOT EXISTS usage_request_admissions (
    `request_id` VARCHAR(128) NOT NULL,
    `subject_id` VARCHAR(128) NOT NULL,
    `event_token` VARCHAR(128) NOT NULL,
    `admitted_at` BIGINT NOT NULL,
    `retain_until` BIGINT NOT NULL,
    `state` VARCHAR(20) NOT NULL,
    `released_at` BIGINT,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`event_token`),
    CONSTRAINT usage_request_admissions_retention_check
        CHECK (`retain_until` > `admitted_at`),
    CONSTRAINT usage_request_admissions_state_check
        CHECK (`state` IN ('active', 'released')),
    CONSTRAINT usage_request_admissions_lifecycle_check CHECK (
        (`state` = 'active' AND `released_at` IS NULL)
        OR (`state` = 'released' AND `released_at` IS NOT NULL
            AND `released_at` >= `admitted_at`)
    ),
    KEY usage_request_admissions_subject_admitted_at_idx (`subject_id`, `admitted_at`),
    KEY usage_request_admissions_retain_until_token_idx (`retain_until`, `event_token`)
);
