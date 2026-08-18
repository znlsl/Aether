-- Preserve the already-published cost-ledger migration checksum. This
-- follow-up also upgrades databases which ran the feature branch before user
-- ownership was enforced. Keep one ALTER TABLE per migration because MySQL
-- DDL implicitly commits.
DELETE reservation
FROM usage_cost_reservations AS reservation
LEFT JOIN users AS app_user ON app_user.id = reservation.subject_id
WHERE app_user.id IS NULL;

ALTER TABLE usage_cost_reservations
    ADD CONSTRAINT usage_cost_reservations_subject_id_fkey
    FOREIGN KEY (`subject_id`) REFERENCES users (`id`) ON DELETE CASCADE;
