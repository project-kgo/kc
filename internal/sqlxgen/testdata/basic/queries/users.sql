-- name: FindActive
SELECT id, tenant_id, email, status, created_at, deleted_at
FROM app.users
WHERE tenant_id = :tenant_id AND status = :status;

-- name: CountAll
SELECT COUNT(*) FROM app.users;

-- name: Disable
UPDATE app.users SET status = 'disabled'
WHERE tenant_id = :tenant_id AND status = :status;
