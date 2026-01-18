# ADR-003: Configuration Management

## Status
Accepted

## Context

CeleRS needs a flexible configuration system that:
1. Works across different deployment environments (dev, staging, production)
2. Follows 12-Factor App principles
3. Is compatible with Celery's configuration style
4. Supports both CLI arguments and file-based configuration

### Requirements

- Environment variable support (`CELERY_BROKER_URL`, `CELERS_*`)
- Configuration files (TOML/YAML/JSON)
- Command-line argument overrides
- Type-safe configuration structs
- Validation and error reporting

## Decision

We will implement a **layered configuration system** with the following priority (highest to lowest):

1. Command-line arguments
2. Environment variables
3. Configuration file (celers.toml / celers.yaml)
4. Built-in defaults

### Implementation Strategy

```toml
# celers.toml
[broker]
type = "redis"
url = "redis://localhost:6379"
queue = "celers"
mode = "fifo"  # or "priority"

[worker]
concurrency = 4
poll_interval_ms = 1000
max_retries = 3
default_timeout_secs = 300

queues = ["celers", "high_priority", "low_priority"]

[result_backend]
type = "redis"
url = "redis://localhost:6379"
ttl_seconds = 3600

[beat]
schedule_file = "celerybeat-schedule.db"
max_interval = 300
```

### Technologies

- **Config File Parsing**: Use `config` crate for TOML/YAML/JSON support
- **Environment Variables**: Direct parsing via `std::env` with `CELERS_` prefix
- **CLI Arguments**: `clap` with derive macros
- **Validation**: Custom validators for URLs, ranges, enum values

## Consequences

### Positive

1. **12-Factor Compliance**: Environment variables enable cloud-native deployments
2. **Flexibility**: Multiple configuration sources for different use cases
3. **Compatibility**: `CELERY_*` environment variable names work unchanged
4. **Type Safety**: Rust structs prevent invalid configurations at startup
5. **Documentation**: Configuration structs serve as self-documenting schema

### Negative

1. **Complexity**: Multiple configuration sources require careful precedence rules
2. **Startup Time**: Configuration loading and validation add initialization overhead
3. **Error Reporting**: Must provide clear messages when configuration conflicts

### Example Priority Resolution

```bash
# Priority order example:
# 1. CLI argument overrides all:
celers worker --concurrency 8

# 2. Environment variable overrides file:
export CELERS_WORKER_CONCURRENCY=8
celers worker

# 3. Config file used if no override:
# celers.toml: concurrency = 4

# 4. Default used if nothing specified:
# Default: concurrency = num_cpus
```

### Environment Variable Mapping

```bash
# Nested configuration via underscores:
CELERS_BROKER_URL=redis://...
CELERS_BROKER_TYPE=redis
CELERS_WORKER_CONCURRENCY=8
CELERS_RESULT_BACKEND_TTL_SECONDS=7200

# Legacy Celery compatibility:
CELERY_BROKER_URL=redis://...  # Maps to CELERS_BROKER_URL
```

## References

- [The Twelve-Factor App](https://12factor.net/config)
- [config crate](https://docs.rs/config/)
- Celery: [Configuration Reference](https://docs.celeryq.dev/en/stable/userguide/configuration.html)
- Django-style settings pattern (environment-specific configuration files)
