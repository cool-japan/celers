-- CeleRS Migrations Tracking Table
-- This table tracks which migrations have been applied.
--
-- IMPORTANT: this file is run *untracked* on every `MysqlBroker::migrate()`
-- call, so every statement in it must be idempotent. `CREATE TABLE IF NOT
-- EXISTS` is; a bare `CREATE INDEX` is not, and `CREATE INDEX IF NOT EXISTS`
-- is MariaDB-only syntax that MySQL 8 rejects outright. The UNIQUE constraint
-- on `version` already provides the index that version lookups need, so no
-- separate index statement is required here.

CREATE TABLE IF NOT EXISTS celers_migrations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
