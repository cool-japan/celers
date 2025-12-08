//! In-memory result cache for Redis backend
//!
//! This module provides an optional LRU cache layer that sits in front of Redis
//! to reduce latency and Redis load for frequently accessed task results.
//!
//! # Example
//!
//! ```
//! use celers_backend_redis::cache::{ResultCache, CacheConfig};
//! use std::time::Duration;
//!
//! // Create a cache with 1000 entry capacity and 5 minute TTL
//! let config = CacheConfig::new()
//!     .with_capacity(1000)
//!     .with_ttl(Duration::from_secs(300));
//!
//! let cache = ResultCache::new(config);
//! ```

use crate::TaskMeta;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

/// Cache entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Cached task metadata
    data: TaskMeta,
    /// When this entry was cached
    cached_at: DateTime<Utc>,
    /// Number of times this entry has been accessed
    access_count: u64,
}

impl CacheEntry {
    fn new(data: TaskMeta) -> Self {
        Self {
            data,
            cached_at: Utc::now(),
            access_count: 0,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        let age = Utc::now() - self.cached_at;
        age.num_milliseconds() > ttl.as_millis() as i64
    }

    fn access(&mut self) -> TaskMeta {
        self.access_count += 1;
        self.data.clone()
    }
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries to cache
    pub capacity: usize,

    /// Time-to-live for cache entries
    pub ttl: Duration,

    /// Enable the cache
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 1000,
            ttl: Duration::from_secs(300), // 5 minutes
            enabled: true,
        }
    }
}

impl CacheConfig {
    /// Create new cache configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable the cache
    pub fn disabled() -> Self {
        Self {
            capacity: 0,
            ttl: Duration::from_secs(0),
            enabled: false,
        }
    }

    /// Set cache capacity
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Set cache TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Enable or disable the cache
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// In-memory result cache
///
/// Thread-safe LRU cache with time-based expiration.
#[derive(Debug, Clone)]
pub struct ResultCache {
    config: CacheConfig,
    entries: Arc<RwLock<HashMap<Uuid, CacheEntry>>>,
}

impl Default for ResultCache {
    fn default() -> Self {
        Self::new(CacheConfig::default())
    }
}

impl ResultCache {
    /// Create a new result cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a disabled cache (no-op)
    pub fn disabled() -> Self {
        Self::new(CacheConfig::disabled())
    }

    /// Check if the cache is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the cache configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Get a result from the cache
    ///
    /// Returns `None` if the entry is not in the cache or has expired.
    pub fn get(&self, task_id: Uuid) -> Option<TaskMeta> {
        if !self.config.enabled {
            return None;
        }

        let mut entries = self.entries.write().ok()?;

        if let Some(entry) = entries.get_mut(&task_id) {
            if entry.is_expired(self.config.ttl) {
                // Entry expired, remove it
                entries.remove(&task_id);
                return None;
            }
            // Entry is valid, return it
            Some(entry.access())
        } else {
            None
        }
    }

    /// Put a result into the cache
    ///
    /// If the cache is full, removes the oldest entry.
    pub fn put(&self, task_id: Uuid, meta: TaskMeta) {
        if !self.config.enabled {
            return;
        }

        let mut entries = self.entries.write().ok().unwrap();

        // If at capacity, remove oldest entry
        if entries.len() >= self.config.capacity && !entries.contains_key(&task_id) {
            self.evict_oldest(&mut entries);
        }

        entries.insert(task_id, CacheEntry::new(meta));
    }

    /// Invalidate (remove) a cache entry
    pub fn invalidate(&self, task_id: Uuid) {
        if !self.config.enabled {
            return;
        }

        if let Ok(mut entries) = self.entries.write() {
            entries.remove(&task_id);
        }
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.clear();
        }
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove expired entries from the cache
    ///
    /// This should be called periodically to prevent memory leaks.
    pub fn cleanup_expired(&self) -> usize {
        if !self.config.enabled {
            return 0;
        }

        let mut entries = match self.entries.write() {
            Ok(e) => e,
            Err(_) => return 0,
        };

        let before_count = entries.len();

        // Collect expired keys
        let expired_keys: Vec<Uuid> = entries
            .iter()
            .filter(|(_, entry)| entry.is_expired(self.config.ttl))
            .map(|(key, _)| *key)
            .collect();

        // Remove expired entries
        for key in expired_keys {
            entries.remove(&key);
        }

        before_count - entries.len()
    }

    /// Evict the oldest entry based on cached_at timestamp
    fn evict_oldest(&self, entries: &mut HashMap<Uuid, CacheEntry>) {
        if let Some((&oldest_key, _)) = entries.iter().min_by_key(|(_, entry)| entry.cached_at) {
            entries.remove(&oldest_key);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let entries = match self.entries.read() {
            Ok(e) => e,
            Err(_) => {
                return CacheStats {
                    size: 0,
                    capacity: self.config.capacity,
                    expired_count: 0,
                }
            }
        };

        let expired_count = entries
            .values()
            .filter(|entry| entry.is_expired(self.config.ttl))
            .count();

        CacheStats {
            size: entries.len(),
            capacity: self.config.capacity,
            expired_count,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries in cache
    pub size: usize,
    /// Maximum cache capacity
    pub capacity: usize,
    /// Number of expired entries
    pub expired_count: usize,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cache: {}/{} entries ({} expired)",
            self.size, self.capacity, self.expired_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_config_defaults() {
        let config = CacheConfig::default();
        assert_eq!(config.capacity, 1000);
        assert_eq!(config.ttl.as_secs(), 300);
        assert!(config.enabled);
    }

    #[test]
    fn test_cache_config_builder() {
        let config = CacheConfig::new()
            .with_capacity(500)
            .with_ttl(Duration::from_secs(60))
            .with_enabled(true);

        assert_eq!(config.capacity, 500);
        assert_eq!(config.ttl.as_secs(), 60);
        assert!(config.enabled);
    }

    #[test]
    fn test_cache_disabled() {
        let cache = ResultCache::disabled();
        assert!(!cache.is_enabled());

        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());

        cache.put(task_id, meta.clone());
        assert!(cache.get(task_id).is_none());
    }

    #[test]
    fn test_cache_put_and_get() {
        let cache = ResultCache::new(CacheConfig::new());
        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());

        cache.put(task_id, meta.clone());
        let result = cache.get(task_id);

        assert!(result.is_some());
        assert_eq!(result.unwrap().task_id, task_id);
    }

    #[test]
    fn test_cache_miss() {
        let cache = ResultCache::new(CacheConfig::new());
        let task_id = Uuid::new_v4();

        assert!(cache.get(task_id).is_none());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = ResultCache::new(CacheConfig::new());
        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());

        cache.put(task_id, meta.clone());
        assert!(cache.get(task_id).is_some());

        cache.invalidate(task_id);
        assert!(cache.get(task_id).is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = ResultCache::new(CacheConfig::new());

        for i in 0..10 {
            let task_id = Uuid::new_v4();
            let meta = TaskMeta::new(task_id, format!("test-{}", i));
            cache.put(task_id, meta);
        }

        assert_eq!(cache.len(), 10);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_capacity() {
        let config = CacheConfig::new().with_capacity(5);
        let cache = ResultCache::new(config);

        // Add 10 entries (should only keep 5)
        let mut task_ids = Vec::new();
        for i in 0..10 {
            let task_id = Uuid::new_v4();
            let meta = TaskMeta::new(task_id, format!("test-{}", i));
            cache.put(task_id, meta);
            task_ids.push(task_id);
        }

        assert_eq!(cache.len(), 5);

        // The last 5 entries should be in the cache
        for task_id in task_ids.iter().skip(5) {
            assert!(cache.get(*task_id).is_some());
        }
    }

    #[test]
    fn test_cache_expiration() {
        let config = CacheConfig::new().with_ttl(Duration::from_millis(50));
        let cache = ResultCache::new(config);

        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());
        cache.put(task_id, meta);

        // Should be in cache immediately
        assert!(cache.get(task_id).is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(100));

        // Should be expired now
        assert!(cache.get(task_id).is_none());
    }

    #[test]
    fn test_cache_cleanup_expired() {
        let config = CacheConfig::new().with_ttl(Duration::from_millis(50));
        let cache = ResultCache::new(config);

        // Add entries
        for i in 0..5 {
            let task_id = Uuid::new_v4();
            let meta = TaskMeta::new(task_id, format!("test-{}", i));
            cache.put(task_id, meta);
        }

        assert_eq!(cache.len(), 5);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(100));

        // Cleanup expired entries
        let removed = cache.cleanup_expired();
        assert_eq!(removed, 5);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_stats() {
        let cache = ResultCache::new(CacheConfig::new().with_capacity(10));

        for i in 0..5 {
            let task_id = Uuid::new_v4();
            let meta = TaskMeta::new(task_id, format!("test-{}", i));
            cache.put(task_id, meta);
        }

        let stats = cache.stats();
        assert_eq!(stats.size, 5);
        assert_eq!(stats.capacity, 10);
    }
}
