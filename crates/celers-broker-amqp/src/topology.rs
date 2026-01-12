//! AMQP topology validation and management utilities
//!
//! This module provides utilities for validating, analyzing, and managing
//! AMQP topology (exchanges, queues, bindings).
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::topology::{TopologyValidator, QueueDefinition, ExchangeDefinition};
//! use celers_broker_amqp::AmqpExchangeType;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut validator = TopologyValidator::new();
//!
//! // Add exchange definition
//! let exchange = ExchangeDefinition {
//!     name: "my_exchange".to_string(),
//!     exchange_type: AmqpExchangeType::Direct,
//!     durable: true,
//!     auto_delete: false,
//! };
//! validator.add_exchange(exchange)?;
//!
//! // Add queue definition
//! let queue = QueueDefinition {
//!     name: "my_queue".to_string(),
//!     durable: true,
//!     auto_delete: false,
//!     bindings: vec![],
//! };
//! validator.add_queue(queue)?;
//!
//! // Validate topology
//! validator.validate()?;
//! # Ok(())
//! # }
//! ```

use crate::AmqpExchangeType;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Exchange definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeDefinition {
    /// Exchange name
    pub name: String,
    /// Exchange type
    pub exchange_type: AmqpExchangeType,
    /// Durability
    pub durable: bool,
    /// Auto-delete flag
    pub auto_delete: bool,
}

/// Queue binding definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindingDefinition {
    /// Source exchange
    pub exchange: String,
    /// Routing key
    pub routing_key: String,
}

/// Queue definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDefinition {
    /// Queue name
    pub name: String,
    /// Durability
    pub durable: bool,
    /// Auto-delete flag
    pub auto_delete: bool,
    /// Bindings
    pub bindings: Vec<BindingDefinition>,
}

/// Topology validation error
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    /// Duplicate exchange definition
    #[error("Duplicate exchange: {0}")]
    DuplicateExchange(String),

    /// Duplicate queue definition
    #[error("Duplicate queue: {0}")]
    DuplicateQueue(String),

    /// Exchange not found
    #[error("Exchange not found: {0}")]
    ExchangeNotFound(String),

    /// Queue not found
    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    /// Invalid binding
    #[error("Invalid binding: {0}")]
    InvalidBinding(String),

    /// Circular dependency detected
    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    /// Reserved name usage
    #[error("Reserved name cannot be used: {0}")]
    ReservedName(String),
}

/// Topology validator
#[derive(Debug, Clone)]
pub struct TopologyValidator {
    exchanges: HashMap<String, ExchangeDefinition>,
    queues: HashMap<String, QueueDefinition>,
}

impl TopologyValidator {
    /// Create a new topology validator
    pub fn new() -> Self {
        Self {
            exchanges: HashMap::new(),
            queues: HashMap::new(),
        }
    }

    /// Add exchange definition
    pub fn add_exchange(&mut self, exchange: ExchangeDefinition) -> Result<(), TopologyError> {
        // Check for reserved names
        if Self::is_reserved_exchange(&exchange.name) {
            return Err(TopologyError::ReservedName(exchange.name.clone()));
        }

        // Check for duplicates
        if self.exchanges.contains_key(&exchange.name) {
            return Err(TopologyError::DuplicateExchange(exchange.name.clone()));
        }

        self.exchanges.insert(exchange.name.clone(), exchange);
        Ok(())
    }

    /// Add queue definition
    pub fn add_queue(&mut self, queue: QueueDefinition) -> Result<(), TopologyError> {
        // Check for reserved names
        if Self::is_reserved_queue(&queue.name) {
            return Err(TopologyError::ReservedName(queue.name.clone()));
        }

        // Check for duplicates
        if self.queues.contains_key(&queue.name) {
            return Err(TopologyError::DuplicateQueue(queue.name.clone()));
        }

        self.queues.insert(queue.name.clone(), queue);
        Ok(())
    }

    /// Validate entire topology
    pub fn validate(&self) -> Result<(), TopologyError> {
        // Validate all bindings reference existing exchanges
        for queue in self.queues.values() {
            for binding in &queue.bindings {
                if !binding.exchange.is_empty() && !self.exchanges.contains_key(&binding.exchange) {
                    return Err(TopologyError::ExchangeNotFound(binding.exchange.clone()));
                }
            }
        }

        // Additional validations can be added here
        Ok(())
    }

    /// Get topology summary
    pub fn summary(&self) -> TopologySummary {
        let total_bindings: usize = self.queues.values().map(|q| q.bindings.len()).sum();

        TopologySummary {
            total_exchanges: self.exchanges.len(),
            total_queues: self.queues.len(),
            total_bindings,
            durable_exchanges: self.exchanges.values().filter(|e| e.durable).count(),
            durable_queues: self.queues.values().filter(|q| q.durable).count(),
        }
    }

    /// Find unbound queues (queues with no bindings)
    pub fn find_unbound_queues(&self) -> Vec<String> {
        self.queues
            .values()
            .filter(|q| q.bindings.is_empty())
            .map(|q| q.name.clone())
            .collect()
    }

    /// Find unused exchanges (exchanges with no bindings)
    pub fn find_unused_exchanges(&self) -> Vec<String> {
        let bound_exchanges: HashSet<String> = self
            .queues
            .values()
            .flat_map(|q| q.bindings.iter().map(|b| b.exchange.clone()))
            .collect();

        self.exchanges
            .keys()
            .filter(|name| !bound_exchanges.contains(*name))
            .cloned()
            .collect()
    }

    /// Check if exchange name is reserved
    fn is_reserved_exchange(name: &str) -> bool {
        name.is_empty() || name.starts_with("amq.")
    }

    /// Check if queue name is reserved
    fn is_reserved_queue(name: &str) -> bool {
        name.is_empty() || name.starts_with("amq.")
    }

    /// Get all exchanges
    pub fn get_exchanges(&self) -> &HashMap<String, ExchangeDefinition> {
        &self.exchanges
    }

    /// Get all queues
    pub fn get_queues(&self) -> &HashMap<String, QueueDefinition> {
        &self.queues
    }
}

impl Default for TopologyValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Topology summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologySummary {
    /// Total number of exchanges
    pub total_exchanges: usize,
    /// Total number of queues
    pub total_queues: usize,
    /// Total number of bindings
    pub total_bindings: usize,
    /// Number of durable exchanges
    pub durable_exchanges: usize,
    /// Number of durable queues
    pub durable_queues: usize,
}

/// Calculate topology complexity score
///
/// Returns a score indicating topology complexity (0-100)
/// Higher scores indicate more complex topologies
///
/// # Arguments
///
/// * `exchanges` - Number of exchanges
/// * `queues` - Number of queues
/// * `bindings` - Number of bindings
///
/// # Returns
///
/// Complexity score (0-100)
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::topology::calculate_topology_complexity;
///
/// let score = calculate_topology_complexity(5, 10, 15);
/// assert!(score >= 0.0 && score <= 100.0);
/// ```
pub fn calculate_topology_complexity(exchanges: usize, queues: usize, bindings: usize) -> f64 {
    // Simple complexity formula:
    // - More exchanges/queues increase complexity
    // - More bindings increase complexity
    // - Normalize to 0-100 scale

    let base_score = (exchanges as f64 * 2.0) + (queues as f64 * 1.5) + (bindings as f64 * 1.0);

    // Normalize to 0-100 (assuming 100 as a very complex topology baseline)
    (base_score / 100.0 * 100.0).min(100.0)
}

/// Validate queue naming convention
///
/// # Arguments
///
/// * `name` - Queue name to validate
/// * `pattern` - Expected naming pattern (supports wildcards)
///
/// # Returns
///
/// True if name matches pattern
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::topology::validate_queue_naming;
///
/// assert!(validate_queue_naming("task.high", "task.*"));
/// assert!(!validate_queue_naming("other.queue", "task.*"));
/// ```
pub fn validate_queue_naming(name: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return name.starts_with(prefix);
    }

    if let Some(suffix) = pattern.strip_prefix('*') {
        return name.ends_with(suffix);
    }

    name == pattern
}

/// Analyze topology for potential issues
///
/// # Arguments
///
/// * `validator` - Topology validator with definitions
///
/// # Returns
///
/// List of potential issues found
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::topology::{TopologyValidator, analyze_topology_issues};
///
/// let validator = TopologyValidator::new();
/// let issues = analyze_topology_issues(&validator);
/// ```
pub fn analyze_topology_issues(validator: &TopologyValidator) -> Vec<String> {
    let mut issues = Vec::new();

    // Check for unbound queues
    let unbound = validator.find_unbound_queues();
    if !unbound.is_empty() {
        issues.push(format!("Unbound queues found: {:?}", unbound));
    }

    // Check for unused exchanges
    let unused = validator.find_unused_exchanges();
    if !unused.is_empty() {
        issues.push(format!("Unused exchanges found: {:?}", unused));
    }

    // Check for auto-delete durable resources (contradictory)
    for exchange in validator.get_exchanges().values() {
        if exchange.durable && exchange.auto_delete {
            issues.push(format!(
                "Exchange '{}' is both durable and auto-delete",
                exchange.name
            ));
        }
    }

    for queue in validator.get_queues().values() {
        if queue.durable && queue.auto_delete {
            issues.push(format!(
                "Queue '{}' is both durable and auto-delete",
                queue.name
            ));
        }
    }

    issues
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_exchange() {
        let mut validator = TopologyValidator::new();

        let exchange = ExchangeDefinition {
            name: "test_exchange".to_string(),
            exchange_type: AmqpExchangeType::Direct,
            durable: true,
            auto_delete: false,
        };

        assert!(validator.add_exchange(exchange).is_ok());
    }

    #[test]
    fn test_duplicate_exchange() {
        let mut validator = TopologyValidator::new();

        let exchange = ExchangeDefinition {
            name: "test_exchange".to_string(),
            exchange_type: AmqpExchangeType::Direct,
            durable: true,
            auto_delete: false,
        };

        validator.add_exchange(exchange.clone()).unwrap();

        let result = validator.add_exchange(exchange);
        assert!(matches!(result, Err(TopologyError::DuplicateExchange(_))));
    }

    #[test]
    fn test_reserved_exchange_name() {
        let mut validator = TopologyValidator::new();

        let exchange = ExchangeDefinition {
            name: "amq.direct".to_string(),
            exchange_type: AmqpExchangeType::Direct,
            durable: true,
            auto_delete: false,
        };

        let result = validator.add_exchange(exchange);
        assert!(matches!(result, Err(TopologyError::ReservedName(_))));
    }

    #[test]
    fn test_add_queue() {
        let mut validator = TopologyValidator::new();

        let queue = QueueDefinition {
            name: "test_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![],
        };

        assert!(validator.add_queue(queue).is_ok());
    }

    #[test]
    fn test_validate_topology() {
        let mut validator = TopologyValidator::new();

        let exchange = ExchangeDefinition {
            name: "test_exchange".to_string(),
            exchange_type: AmqpExchangeType::Direct,
            durable: true,
            auto_delete: false,
        };
        validator.add_exchange(exchange).unwrap();

        let queue = QueueDefinition {
            name: "test_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![BindingDefinition {
                exchange: "test_exchange".to_string(),
                routing_key: "test".to_string(),
            }],
        };
        validator.add_queue(queue).unwrap();

        assert!(validator.validate().is_ok());
    }

    #[test]
    fn test_validate_missing_exchange() {
        let mut validator = TopologyValidator::new();

        let queue = QueueDefinition {
            name: "test_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![BindingDefinition {
                exchange: "nonexistent_exchange".to_string(),
                routing_key: "test".to_string(),
            }],
        };
        validator.add_queue(queue).unwrap();

        let result = validator.validate();
        assert!(matches!(result, Err(TopologyError::ExchangeNotFound(_))));
    }

    #[test]
    fn test_find_unbound_queues() {
        let mut validator = TopologyValidator::new();

        let queue1 = QueueDefinition {
            name: "bound_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![BindingDefinition {
                exchange: "exchange".to_string(),
                routing_key: "key".to_string(),
            }],
        };
        validator.add_queue(queue1).unwrap();

        let queue2 = QueueDefinition {
            name: "unbound_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![],
        };
        validator.add_queue(queue2).unwrap();

        let unbound = validator.find_unbound_queues();
        assert_eq!(unbound.len(), 1);
        assert_eq!(unbound[0], "unbound_queue");
    }

    #[test]
    fn test_calculate_topology_complexity() {
        let score = calculate_topology_complexity(5, 10, 15);
        assert!((0.0..=100.0).contains(&score));

        let simple_score = calculate_topology_complexity(1, 1, 1);
        let complex_score = calculate_topology_complexity(20, 50, 100);
        assert!(simple_score < complex_score);
    }

    #[test]
    fn test_validate_queue_naming() {
        assert!(validate_queue_naming("task.high", "task.*"));
        assert!(validate_queue_naming("task.low", "task.*"));
        assert!(!validate_queue_naming("other.queue", "task.*"));

        assert!(validate_queue_naming("anything", "*"));

        assert!(validate_queue_naming("queue.celery", "*.celery"));
        assert!(!validate_queue_naming("queue.other", "*.celery"));
    }

    #[test]
    fn test_topology_summary() {
        let mut validator = TopologyValidator::new();

        let exchange = ExchangeDefinition {
            name: "test_exchange".to_string(),
            exchange_type: AmqpExchangeType::Direct,
            durable: true,
            auto_delete: false,
        };
        validator.add_exchange(exchange).unwrap();

        let queue = QueueDefinition {
            name: "test_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![BindingDefinition {
                exchange: "test_exchange".to_string(),
                routing_key: "key".to_string(),
            }],
        };
        validator.add_queue(queue).unwrap();

        let summary = validator.summary();
        assert_eq!(summary.total_exchanges, 1);
        assert_eq!(summary.total_queues, 1);
        assert_eq!(summary.total_bindings, 1);
        assert_eq!(summary.durable_exchanges, 1);
        assert_eq!(summary.durable_queues, 1);
    }

    #[test]
    fn test_analyze_topology_issues() {
        let mut validator = TopologyValidator::new();

        // Add unbound queue
        let queue = QueueDefinition {
            name: "unbound_queue".to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![],
        };
        validator.add_queue(queue).unwrap();

        let issues = analyze_topology_issues(&validator);
        assert!(!issues.is_empty());
    }
}
