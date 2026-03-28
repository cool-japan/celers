/// Validation error
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// Invalid configuration error
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Missing required field error
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// Invalid value for a specific field
    #[error("Invalid value for {field}: {message}")]
    InvalidValue {
        /// The field name that has an invalid value
        field: String,
        /// Error message describing why the value is invalid
        message: String,
    },

    /// Incompatible configuration detected
    #[error("Incompatible configuration: {0}")]
    IncompatibleConfig(String),
}

/// Configuration validator
pub struct ConfigValidator {
    errors: Vec<ValidationError>,
    warnings: Vec<String>,
}

impl ConfigValidator {
    /// Create a new validator
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Check if a required field is present
    pub fn require_field(&mut self, field_name: &str, value: Option<&str>) {
        if value.is_none() || value == Some("") {
            self.errors
                .push(ValidationError::MissingField(field_name.to_string()));
        }
    }

    /// Add a validation error
    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    /// Add a warning
    pub fn add_warning(&mut self, message: String) {
        self.warnings.push(message);
    }

    /// Check if validation passed
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get all errors
    pub fn errors(&self) -> &[ValidationError] {
        &self.errors
    }

    /// Get all warnings
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Validate and return result
    pub fn validate(self) -> Result<Vec<String>, Vec<ValidationError>> {
        if self.errors.is_empty() {
            Ok(self.warnings)
        } else {
            Err(self.errors)
        }
    }
}

impl Default for ConfigValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate worker configuration
pub fn validate_worker_config(
    concurrency: Option<usize>,
    prefetch_count: Option<usize>,
) -> Result<Vec<String>, Vec<ValidationError>> {
    let mut validator = ConfigValidator::new();

    if let Some(c) = concurrency {
        if c == 0 {
            validator.add_error(ValidationError::InvalidValue {
                field: "concurrency".to_string(),
                message: "must be greater than 0".to_string(),
            });
        }
        if c > 1000 {
            validator.add_warning(format!(
                "High concurrency value ({}). Consider if this is intentional.",
                c
            ));
        }
    }

    if let Some(p) = prefetch_count {
        if p == 0 {
            validator.add_error(ValidationError::InvalidValue {
                field: "prefetch_count".to_string(),
                message: "must be greater than 0".to_string(),
            });
        }
        if p > 1000 {
            validator.add_warning(format!(
                "High prefetch_count value ({}). This may consume significant memory.",
                p
            ));
        }
    }

    validator.validate()
}

/// Validate broker URL format
pub fn validate_broker_url(url: &str) -> Result<String, ValidationError> {
    if url.is_empty() {
        return Err(ValidationError::InvalidValue {
            field: "broker_url".to_string(),
            message: "cannot be empty".to_string(),
        });
    }

    // Basic URL format validation
    if !url.contains("://") {
        return Err(ValidationError::InvalidValue {
            field: "broker_url".to_string(),
            message: "invalid URL format (missing scheme)".to_string(),
        });
    }

    // Extract scheme
    let scheme = url.split("://").next().unwrap_or("");

    // Validate known schemes
    match scheme {
        "redis" | "rediss" | "postgres" | "postgresql" | "mysql" | "amqp" | "amqps" | "sqs" => {
            Ok(format!("Valid {} URL", scheme))
        }
        _ => Err(ValidationError::InvalidValue {
            field: "broker_url".to_string(),
            message: format!("unsupported scheme: {}", scheme),
        }),
    }
}

/// Check feature compatibility
pub fn check_feature_compatibility(features: &[&str]) -> Result<Vec<String>, ValidationError> {
    let mut warnings = Vec::new();

    // Check for multiple broker features enabled
    let broker_features: Vec<_> = features
        .iter()
        .filter(|f| ["redis", "postgres", "mysql", "amqp", "sqs"].contains(f))
        .collect();

    if broker_features.len() > 1 {
        warnings.push(format!(
            "Multiple broker features enabled: {:?}. Ensure you're using the correct broker.",
            broker_features
        ));
    }

    // Check for multiple backend features enabled
    let backend_features: Vec<_> = features
        .iter()
        .filter(|f| ["backend-redis", "backend-db", "backend-rpc"].contains(f))
        .collect();

    if backend_features.len() > 1 {
        warnings.push(format!(
            "Multiple backend features enabled: {:?}. Ensure you're using the correct backend.",
            backend_features
        ));
    }

    Ok(warnings)
}

/// Get feature compatibility matrix documentation
///
/// Returns a formatted string documenting which features are compatible
/// and which combinations are recommended.
///
/// # Example
///
/// ```rust
/// use celers::config_validation::feature_compatibility_matrix;
///
/// println!("{}", feature_compatibility_matrix());
/// ```
pub fn feature_compatibility_matrix() -> String {
    r#"
╔══════════════════════════════════════════════════════════════════════════════╗
║                      CeleRS Feature Compatibility Matrix                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

BROKER FEATURES (Choose ONE):
  ✓ redis      - Redis broker (recommended for most use cases)
  ✓ postgres   - PostgreSQL broker (good for existing PostgreSQL infrastructure)
  ✓ mysql      - MySQL broker (good for existing MySQL infrastructure)
  ✓ amqp       - RabbitMQ/AMQP broker (enterprise messaging)
  ✓ sqs        - AWS SQS broker (cloud-native, serverless)

BACKEND FEATURES (Choose ONE):
  ✓ backend-redis  - Redis result backend (recommended with redis broker)
  ✓ backend-db     - PostgreSQL/MySQL backend (use with postgres/mysql broker)
  ✓ backend-rpc    - gRPC result backend (distributed systems)

SERIALIZATION FEATURES (Can combine):
  ✓ json           - JSON serialization (default, always available)
  ✓ msgpack        - MessagePack serialization (compact binary format)

OBSERVABILITY FEATURES (Can combine):
  ✓ metrics        - Prometheus metrics
  ✓ tracing        - OpenTelemetry distributed tracing

OTHER FEATURES (Can combine):
  ✓ beat           - Periodic task scheduler
  ✓ dev-utils      - Development and testing utilities

RECOMMENDED COMBINATIONS:
  1. Simple Setup:
 features = ["redis", "backend-redis", "json"]

  2. Production Ready:
 features = ["redis", "backend-redis", "json", "metrics", "tracing"]

  3. PostgreSQL Stack:
 features = ["postgres", "backend-db", "json", "metrics"]

  4. AWS Cloud:
 features = ["sqs", "backend-rpc", "json", "msgpack", "metrics"]

  5. Full Featured:
 features = ["full"]  # Enables all features

NOTES:
  - Multiple brokers can be compiled but only one should be used at runtime
  - Multiple backends can be compiled but only one should be used at runtime
  - json + msgpack enables both serialization formats
  - metrics + tracing provides comprehensive observability
"#
    .to_string()
}
