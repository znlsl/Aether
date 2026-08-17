CREATE TABLE IF NOT EXISTS usage_cost_reservations (
    `request_id` VARCHAR(128) NOT NULL,
    `subject_id` VARCHAR(128) NOT NULL,
    `reservation_token` VARCHAR(128) NOT NULL,
    `admitted_at` BIGINT NOT NULL,
    `reserved_cost_units` BIGINT NOT NULL,
    `actual_cost_units` BIGINT,
    `state` VARCHAR(20) NOT NULL,
    `reservation_expires_at` BIGINT NOT NULL,
    `retain_until` BIGINT NOT NULL,
    `finalized_at` BIGINT,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    PRIMARY KEY (`reservation_token`),
    CONSTRAINT usage_cost_reservations_state_check
        CHECK (`state` IN ('reserved', 'finalized', 'released')),
    CONSTRAINT usage_cost_reservations_reserved_cost_units_check
        CHECK (`reserved_cost_units` >= 0),
    CONSTRAINT usage_cost_reservations_actual_cost_units_check
        CHECK (`actual_cost_units` IS NULL OR `actual_cost_units` >= 0),
    CONSTRAINT usage_cost_reservations_expiry_check
        CHECK (`reservation_expires_at` > `admitted_at`),
    CONSTRAINT usage_cost_reservations_retention_check
        CHECK (`retain_until` >= `reservation_expires_at`),
    CONSTRAINT usage_cost_reservations_lifecycle_check CHECK (
        (`state` = 'reserved' AND `actual_cost_units` IS NULL AND `finalized_at` IS NULL)
        OR (`state` = 'finalized' AND `actual_cost_units` IS NOT NULL AND `finalized_at` IS NOT NULL)
        OR (`state` = 'released' AND `actual_cost_units` IS NOT NULL
            AND `actual_cost_units` = 0 AND `finalized_at` IS NOT NULL)
    ),
    KEY usage_cost_reservations_request_id_idx (`request_id`),
    KEY usage_cost_reservations_subject_admitted_at_idx (`subject_id`, `admitted_at`),
    KEY usage_cost_reservations_reservation_expires_at_idx (`reservation_expires_at`),
    KEY usage_cost_reservations_retain_until_token_idx (`retain_until`, `reservation_token`)
);
