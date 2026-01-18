//! Redis authorization and access control
//!
//! Provides authorization features including:
//! - Command restrictions
//! - Key namespace isolation
//! - User permissions

use std::collections::HashSet;

/// Authorization policy for Redis operations
#[derive(Debug, Clone, Default)]
pub struct AuthorizationPolicy {
    /// Allowed Redis commands (if None, all commands are allowed)
    pub allowed_commands: Option<HashSet<String>>,
    /// Denied Redis commands (takes precedence over allowed_commands)
    pub denied_commands: Option<HashSet<String>>,
    /// Key namespace prefix (all keys must start with this prefix)
    pub key_namespace: Option<String>,
    /// Maximum key length
    pub max_key_length: Option<usize>,
    /// Maximum value size in bytes
    pub max_value_size: Option<usize>,
}

impl AuthorizationPolicy {
    /// Create a new authorization policy with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow only specific Redis commands
    pub fn allow_commands(mut self, commands: &[&str]) -> Self {
        let mut set = HashSet::new();
        for cmd in commands {
            set.insert(cmd.to_uppercase());
        }
        self.allowed_commands = Some(set);
        self
    }

    /// Deny specific Redis commands
    pub fn deny_commands(mut self, commands: &[&str]) -> Self {
        let mut set = HashSet::new();
        for cmd in commands {
            set.insert(cmd.to_uppercase());
        }
        self.denied_commands = Some(set);
        self
    }

    /// Set key namespace prefix (all keys must start with this)
    pub fn key_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.key_namespace = Some(namespace.into());
        self
    }

    /// Set maximum key length
    pub fn max_key_length(mut self, length: usize) -> Self {
        self.max_key_length = Some(length);
        self
    }

    /// Set maximum value size in bytes
    pub fn max_value_size(mut self, size: usize) -> Self {
        self.max_value_size = Some(size);
        self
    }

    /// Check if a command is allowed
    pub fn is_command_allowed(&self, command: &str) -> bool {
        let cmd_upper = command.to_uppercase();

        // Check denied commands first (takes precedence)
        if let Some(denied) = &self.denied_commands {
            if denied.contains(&cmd_upper) {
                return false;
            }
        }

        // Check allowed commands
        if let Some(allowed) = &self.allowed_commands {
            return allowed.contains(&cmd_upper);
        }

        // If no restrictions, allow all
        true
    }

    /// Check if a key is valid according to the policy
    pub fn is_key_valid(&self, key: &str) -> bool {
        // Check namespace
        if let Some(namespace) = &self.key_namespace {
            if !key.starts_with(namespace) {
                return false;
            }
        }

        // Check key length
        if let Some(max_len) = self.max_key_length {
            if key.len() > max_len {
                return false;
            }
        }

        true
    }

    /// Check if a value size is valid according to the policy
    pub fn is_value_size_valid(&self, size: usize) -> bool {
        if let Some(max_size) = self.max_value_size {
            return size <= max_size;
        }
        true
    }

    /// Enforce namespace on a key (add namespace prefix if not present)
    pub fn enforce_namespace(&self, key: &str) -> String {
        if let Some(namespace) = &self.key_namespace {
            if !key.starts_with(namespace) {
                return format!("{}{}", namespace, key);
            }
        }
        key.to_string()
    }
}

/// User permissions for Redis operations
#[derive(Debug, Clone)]
pub struct UserPermissions {
    /// User identifier
    pub user_id: String,
    /// Authorization policy for this user
    pub policy: AuthorizationPolicy,
    /// Read-only mode (only read commands allowed)
    pub read_only: bool,
}

impl UserPermissions {
    /// Create new user permissions
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            policy: AuthorizationPolicy::default(),
            read_only: false,
        }
    }

    /// Set the authorization policy
    pub fn policy(mut self, policy: AuthorizationPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Enable read-only mode
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Check if a command is allowed for this user
    pub fn can_execute_command(&self, command: &str) -> bool {
        // Check read-only mode
        if self.read_only && !Self::is_read_command(command) {
            return false;
        }

        // Check policy
        self.policy.is_command_allowed(command)
    }

    /// Check if a command is a read-only command
    fn is_read_command(command: &str) -> bool {
        matches!(
            command.to_uppercase().as_str(),
            "GET"
                | "MGET"
                | "STRLEN"
                | "GETRANGE"
                | "LLEN"
                | "LRANGE"
                | "LINDEX"
                | "SCARD"
                | "SISMEMBER"
                | "SMEMBERS"
                | "ZCARD"
                | "ZCOUNT"
                | "ZRANGE"
                | "ZRANGEBYSCORE"
                | "ZSCORE"
                | "HGET"
                | "HMGET"
                | "HGETALL"
                | "HLEN"
                | "HEXISTS"
                | "KEYS"
                | "EXISTS"
                | "TTL"
                | "TYPE"
                | "PING"
                | "INFO"
        )
    }
}

/// Preset authorization policies
impl AuthorizationPolicy {
    /// Read-only policy (only allows read commands)
    pub fn read_only() -> Self {
        Self::new().allow_commands(&[
            "GET",
            "MGET",
            "STRLEN",
            "GETRANGE",
            "LLEN",
            "LRANGE",
            "LINDEX",
            "SCARD",
            "SISMEMBER",
            "SMEMBERS",
            "ZCARD",
            "ZCOUNT",
            "ZRANGE",
            "ZRANGEBYSCORE",
            "ZSCORE",
            "HGET",
            "HMGET",
            "HGETALL",
            "HLEN",
            "HEXISTS",
            "KEYS",
            "EXISTS",
            "TTL",
            "TYPE",
            "PING",
            "INFO",
        ])
    }

    /// Queue operations only (for workers)
    pub fn queue_only() -> Self {
        Self::new().allow_commands(&[
            "LPUSH",
            "RPUSH",
            "LPOP",
            "RPOP",
            "BRPOPLPUSH",
            "LLEN",
            "LRANGE",
            "LREM",
            "ZADD",
            "ZPOPMIN",
            "ZCARD",
            "ZRANGE",
            "ZRANGEBYSCORE",
            "ZREM",
            "EXPIRE",
            "TTL",
            "PING",
        ])
    }

    /// Restricted policy (deny dangerous commands)
    pub fn restricted() -> Self {
        Self::new().deny_commands(&[
            "FLUSHDB",
            "FLUSHALL",
            "KEYS",
            "CONFIG",
            "SHUTDOWN",
            "BGREWRITEAOF",
            "BGSAVE",
            "SAVE",
            "DEBUG",
            "MIGRATE",
            "SLAVEOF",
            "REPLICAOF",
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authorization_policy_default() {
        let policy = AuthorizationPolicy::default();
        assert!(policy.allowed_commands.is_none());
        assert!(policy.denied_commands.is_none());
        assert!(policy.key_namespace.is_none());
    }

    #[test]
    fn test_allow_commands() {
        let policy = AuthorizationPolicy::new().allow_commands(&["GET", "SET", "DEL"]);

        assert!(policy.is_command_allowed("GET"));
        assert!(policy.is_command_allowed("set")); // Case insensitive
        assert!(policy.is_command_allowed("DEL"));
        assert!(!policy.is_command_allowed("FLUSHDB"));
    }

    #[test]
    fn test_deny_commands() {
        let policy = AuthorizationPolicy::new().deny_commands(&["FLUSHDB", "FLUSHALL"]);

        assert!(!policy.is_command_allowed("FLUSHDB"));
        assert!(!policy.is_command_allowed("flushall"));
        assert!(policy.is_command_allowed("GET")); // Not denied
    }

    #[test]
    fn test_deny_takes_precedence() {
        let policy = AuthorizationPolicy::new()
            .allow_commands(&["GET", "SET", "DEL"])
            .deny_commands(&["DEL"]);

        assert!(policy.is_command_allowed("GET"));
        assert!(policy.is_command_allowed("SET"));
        assert!(!policy.is_command_allowed("DEL")); // Denied takes precedence
    }

    #[test]
    fn test_key_namespace() {
        let policy = AuthorizationPolicy::new().key_namespace("myapp:");

        assert!(policy.is_key_valid("myapp:user:1"));
        assert!(policy.is_key_valid("myapp:session"));
        assert!(!policy.is_key_valid("other:key"));
        assert!(!policy.is_key_valid("user:1"));
    }

    #[test]
    fn test_max_key_length() {
        let policy = AuthorizationPolicy::new().max_key_length(10);

        assert!(policy.is_key_valid("short"));
        assert!(policy.is_key_valid("1234567890"));
        assert!(!policy.is_key_valid("this_is_too_long"));
    }

    #[test]
    fn test_max_value_size() {
        let policy = AuthorizationPolicy::new().max_value_size(100);

        assert!(policy.is_value_size_valid(50));
        assert!(policy.is_value_size_valid(100));
        assert!(!policy.is_value_size_valid(101));
    }

    #[test]
    fn test_enforce_namespace() {
        let policy = AuthorizationPolicy::new().key_namespace("app:");

        assert_eq!(policy.enforce_namespace("user:1"), "app:user:1");
        assert_eq!(policy.enforce_namespace("app:user:1"), "app:user:1");
    }

    #[test]
    fn test_user_permissions() {
        let perms = UserPermissions::new("user123")
            .policy(AuthorizationPolicy::new().allow_commands(&["GET", "SET"]))
            .read_only(false);

        assert_eq!(perms.user_id, "user123");
        assert!(perms.can_execute_command("GET"));
        assert!(perms.can_execute_command("SET"));
        assert!(!perms.can_execute_command("DEL"));
    }

    #[test]
    fn test_read_only_mode() {
        let perms = UserPermissions::new("user123").read_only(true);

        assert!(perms.can_execute_command("GET"));
        assert!(perms.can_execute_command("LRANGE"));
        assert!(!perms.can_execute_command("SET"));
        assert!(!perms.can_execute_command("DEL"));
    }

    #[test]
    fn test_preset_read_only_policy() {
        let policy = AuthorizationPolicy::read_only();

        assert!(policy.is_command_allowed("GET"));
        assert!(policy.is_command_allowed("LRANGE"));
        assert!(!policy.is_command_allowed("SET"));
        assert!(!policy.is_command_allowed("DEL"));
    }

    #[test]
    fn test_preset_queue_only_policy() {
        let policy = AuthorizationPolicy::queue_only();

        assert!(policy.is_command_allowed("LPUSH"));
        assert!(policy.is_command_allowed("RPUSH"));
        assert!(policy.is_command_allowed("BRPOPLPUSH"));
        assert!(!policy.is_command_allowed("SET"));
        assert!(!policy.is_command_allowed("FLUSHDB"));
    }

    #[test]
    fn test_preset_restricted_policy() {
        let policy = AuthorizationPolicy::restricted();

        assert!(!policy.is_command_allowed("FLUSHDB"));
        assert!(!policy.is_command_allowed("FLUSHALL"));
        assert!(!policy.is_command_allowed("CONFIG"));
        assert!(policy.is_command_allowed("GET"));
        assert!(policy.is_command_allowed("SET"));
    }
}
