-- CeleRS Migrations Tracking Table
-- This table tracks which migrations have been applied

CREATE TABLE IF NOT EXISTS celers_migrations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for version lookups
CREATE INDEX IF NOT EXISTS idx_migrations_version ON celers_migrations(version);
