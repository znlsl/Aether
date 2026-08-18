-- Split from the cost-ledger foreign key so a MySQL implicit DDL commit cannot
-- leave two table changes behind one dirty migration record.
DELETE admission
FROM usage_request_admissions AS admission
LEFT JOIN users AS app_user ON app_user.id = admission.subject_id
WHERE app_user.id IS NULL;

ALTER TABLE usage_request_admissions
    ADD CONSTRAINT usage_request_admissions_subject_id_fkey
    FOREIGN KEY (`subject_id`) REFERENCES users (`id`) ON DELETE CASCADE;
