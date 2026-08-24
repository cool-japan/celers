//! Append-only, JSON-lines [`EventStorage`] backed by a file.
//!
//! Split out of [`crate::event`] to keep that module a manageable size.
//!
//! The store is an audit trail, so it is deliberately conservative about lines
//! it cannot parse (a record truncated by a crash, a record written by a newer
//! schema): they are counted, logged, and — crucially — *preserved* by
//! [`FileEventStorage::cleanup`] instead of being silently destroyed by a
//! routine retention pass.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use super::{Event, EventFilter, EventStorage};

/// One line of the on-disk log: the raw text plus its parse result.
#[derive(Debug)]
struct StoredLine {
    /// The exact bytes (as text) that were read from the file.
    raw: String,
    /// The parsed event, or `None` when the line could not be deserialized.
    event: Option<Event>,
}

/// File-based event storage (append-only JSON lines)
pub struct FileEventStorage {
    path: PathBuf,
    file_handle: Arc<RwLock<Option<tokio::fs::File>>>,
    /// Number of lines that could not be parsed during the most recent read.
    unparseable_lines: Arc<AtomicU64>,
}

impl FileEventStorage {
    /// Create a new file-based event storage
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file_handle: Arc::new(RwLock::new(None)),
            unparseable_lines: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of unparseable lines seen during the most recent full read.
    ///
    /// A non-zero value means the file contains records this build could not
    /// deserialize. They are preserved (never dropped by `cleanup`), but they
    /// are also invisible to [`EventStorage::query`].
    #[must_use]
    pub fn unparseable_lines(&self) -> u64 {
        self.unparseable_lines.load(Ordering::Relaxed)
    }

    /// Initialize the storage file
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or created.
    pub async fn init(&self) -> crate::Result<()> {
        let mut handle = self.file_handle.write().await;
        *handle = Some(Self::open_append(&self.path).await?);
        Ok(())
    }

    /// Open (creating if necessary) the log file for appending.
    async fn open_append(path: &Path) -> crate::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to open event file: {e}")))
    }

    /// Read every line of the file, keeping the raw text alongside the parse
    /// result so callers can round-trip records they cannot understand.
    async fn read_lines(&self) -> crate::Result<Vec<StoredLine>> {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let file = match tokio::fs::File::open(&self.path).await {
            Ok(file) => file,
            // A store that has never been written to simply has no events.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(crate::CelersError::Broker(format!(
                    "Failed to open event file: {e}"
                )))
            }
        };

        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut out = Vec::new();
        let mut unparseable = 0u64;
        let mut line_number = 0u64;

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to read line: {e}")))?
        {
            line_number += 1;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Event>(&line) {
                Ok(event) => out.push(StoredLine {
                    raw: line,
                    event: Some(event),
                }),
                Err(e) => {
                    unparseable += 1;
                    tracing::warn!(
                        path = ?self.path,
                        line = line_number,
                        error = %e,
                        "Unparseable event record; it is preserved on disk but \
                         cannot be returned by queries"
                    );
                    out.push(StoredLine {
                        raw: line,
                        event: None,
                    });
                }
            }
        }

        self.unparseable_lines.store(unparseable, Ordering::Relaxed);
        Ok(out)
    }

    /// Read all *parseable* events from the file.
    async fn read_all(&self) -> crate::Result<Vec<Event>> {
        Ok(self
            .read_lines()
            .await?
            .into_iter()
            .filter_map(|line| line.event)
            .collect())
    }

    /// Serialize an event to a single JSON-lines record.
    fn encode(event: &Event) -> crate::Result<String> {
        serde_json::to_string(event).map_err(|e| crate::CelersError::Serialization(e.to_string()))
    }
}

#[async_trait]
impl EventStorage for FileEventStorage {
    async fn store(&self, event: &Event) -> crate::Result<()> {
        use tokio::io::AsyncWriteExt;

        let json = Self::encode(event)?;

        let mut handle = self.file_handle.write().await;
        if handle.is_none() {
            *handle = Some(Self::open_append(&self.path).await?);
        }

        if let Some(file) = handle.as_mut() {
            file.write_all(json.as_bytes())
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write event: {e}")))?;
            file.write_all(b"\n")
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write newline: {e}")))?;
            file.flush()
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to flush: {e}")))?;
        }

        Ok(())
    }

    async fn query(&self, filter: &EventFilter, limit: Option<usize>) -> crate::Result<Vec<Event>> {
        let all_events = self.read_all().await?;
        let mut filtered: Vec<Event> = all_events
            .into_iter()
            .filter(|e| filter.matches(e))
            .collect();

        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    async fn query_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<usize>,
    ) -> crate::Result<Vec<Event>> {
        let all_events = self.read_all().await?;
        let mut filtered: Vec<Event> = all_events
            .into_iter()
            .filter(|e| {
                let timestamp = e.timestamp();
                timestamp >= start && timestamp <= end
            })
            .collect();

        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    async fn cleanup(&self, before: DateTime<Utc>) -> crate::Result<usize> {
        use tokio::io::AsyncWriteExt;

        // Hold the file-handle write lock for the whole read/rewrite/rename
        // sequence: `store` takes the same lock, so no event can be appended to
        // the inode that is about to be replaced (and therefore lost).
        let mut handle = self.file_handle.write().await;
        if let Some(file) = handle.as_mut() {
            file.flush()
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to flush: {e}")))?;
        }
        *handle = None;

        let lines = self.read_lines().await?;
        let mut keep: Vec<String> = Vec::with_capacity(lines.len());
        let mut removed_count = 0usize;

        for line in lines {
            match &line.event {
                // Parsed and old enough: this is what retention removes.
                Some(event) if event.timestamp() < before => removed_count += 1,
                // Parsed and still within retention.
                Some(_) => keep.push(line.raw),
                // Unparseable: never destroy an audit record we could not read.
                None => keep.push(line.raw),
            }
        }

        // Rewrite the file with the retained lines.
        let temp_path = self.path.with_extension("tmp");
        let mut temp_file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to create temp file: {e}")))?;

        for raw in keep {
            temp_file
                .write_all(raw.as_bytes())
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write: {e}")))?;
            temp_file
                .write_all(b"\n")
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write newline: {e}")))?;
        }

        temp_file
            .flush()
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to flush: {e}")))?;
        drop(temp_file);

        // Replace original file with temp file.
        tokio::fs::rename(&temp_path, &self.path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to rename file: {e}")))?;

        // Reopen against the new inode before releasing the lock.
        *handle = Some(Self::open_append(&self.path).await?);

        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::TaskEvent;
    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("celers_events_{tag}_{}.jsonl", Uuid::new_v4()))
    }

    fn task_event(at: DateTime<Utc>) -> Event {
        Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: "t".to_string(),
            hostname: "w1".to_string(),
            timestamp: at,
            pid: 1,
        })
    }

    #[tokio::test]
    async fn cleanup_preserves_unparseable_lines() {
        // Regression: `cleanup` rebuilt the file from parsed events only, so a
        // truncated or newer-schema record was destroyed by a routine
        // retention pass.
        let path = temp_path("cleanup_preserve");
        let storage = FileEventStorage::new(&path);

        let old = Utc::now() - chrono::Duration::days(10);
        let fresh = Utc::now();
        storage.store(&task_event(old)).await.unwrap();
        storage.store(&task_event(fresh)).await.unwrap();

        // Append a record this build cannot understand (e.g. written by a newer
        // schema) plus a half-written line from a crash.
        {
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await
                .unwrap();
            file.write_all(b"{\"type\":\"task-teleported\",\"task_id\":\"x\"}\n")
                .await
                .unwrap();
            file.write_all(b"{\"type\":\"task-star\n").await.unwrap();
            file.flush().await.unwrap();
        }

        let removed = storage
            .cleanup(Utc::now() - chrono::Duration::days(1))
            .await
            .unwrap();
        assert_eq!(removed, 1, "only the old, parseable event is removed");

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(
            content.contains("task-teleported"),
            "unknown-schema record must survive retention: {content}"
        );
        assert!(
            content.contains("task-star"),
            "partially-written record must survive retention: {content}"
        );
        assert_eq!(storage.unparseable_lines(), 2);

        // The remaining parseable event is still queryable.
        let events = storage.query(&EventFilter::All, None).await.unwrap();
        assert_eq!(events.len(), 1);

        // Writes after a cleanup land in the new file, not the replaced inode.
        storage.store(&task_event(Utc::now())).await.unwrap();
        let events = storage.query(&EventFilter::All, None).await.unwrap();
        assert_eq!(events.len(), 2);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn query_on_missing_file_is_empty() {
        let storage = FileEventStorage::new(temp_path("missing"));
        assert!(storage
            .query(&EventFilter::All, None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn store_query_and_range_round_trip() {
        let path = temp_path("round_trip");
        let storage = FileEventStorage::new(&path);
        let t0 = Utc::now();
        storage.store(&task_event(t0)).await.unwrap();

        let events = storage.query(&EventFilter::All, None).await.unwrap();
        assert_eq!(events.len(), 1);

        let in_range = storage
            .query_range(
                t0 - chrono::Duration::seconds(1),
                t0 + chrono::Duration::seconds(1),
                None,
            )
            .await
            .unwrap();
        assert_eq!(in_range.len(), 1);
        assert_eq!(storage.unparseable_lines(), 0);

        let _ = tokio::fs::remove_file(&path).await;
    }
}
