//! Partition management methods for PostgreSQL table partitioning

use celers_core::{CelersError, Result};
use chrono::{DateTime, Utc};
use oxisql_core::Connection;

use crate::row_ext::RowExt;
use crate::types::PartitionInfo;
use crate::PostgresBroker;

// Partition Management Methods
impl PostgresBroker {
    /// Create a partition for tasks table for a specific date (monthly partitions)
    ///
    /// This requires that the celers_tasks table is partitioned. See migration 003_partitioning.sql
    /// for details on setting up table partitioning.
    ///
    /// # Arguments
    /// * `partition_date` - Any date within the month to create partition for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::{Utc, Datelike};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Create partition for current month
    /// let current_date = Utc::now().naive_utc().date();
    /// broker.create_partition(current_date).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_partition(&self, partition_date: chrono::NaiveDate) -> Result<String> {
        // `chrono::NaiveDate` has no `oxisql_core::ToSqlValue` impl (only the
        // read-side `FromValue` exists — same asymmetric read/write gap
        // `row_ext.rs` documents for `DateTime<Utc>`). Bound as its ISO 8601
        // text form through a `$1::text::date` cast, the `NaiveDate`
        // analogue of the crate's `DateTime<Utc>` -> `$n::text::timestamptz`
        // convention (`TEXT`'s binary wire format is raw UTF-8 for both
        // cases, so the bind round-trips regardless of the client always
        // using Postgres binary format).
        let date_param = partition_date.to_string();
        let rows = self
            .conn
            .query(
                "SELECT create_tasks_partition($1::text::date)",
                &[&date_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to create partition: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to create partition: no rows returned".to_string())
        })?;

        row.col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read partition result: {}", e)))
    }

    /// Create partitions for a date range (monthly partitions)
    ///
    /// This creates all partitions from start_date to end_date (inclusive, by month).
    /// Useful for initializing partitions for several months ahead.
    ///
    /// # Arguments
    /// * `start_date` - Start of date range
    /// * `end_date` - End of date range
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::{Utc, Datelike, Duration};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Create partitions for next 6 months
    /// let start = Utc::now().naive_utc().date();
    /// let end = start + Duration::days(180);
    /// let results = broker.create_partitions_range(start, end).await?;
    /// println!("Created {} partitions", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_partitions_range(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> Result<Vec<(String, String)>> {
        // Same `NaiveDate` -> `$n::text::date` cast convention as
        // `create_partition` above.
        let start_param = start_date.to_string();
        let end_param = end_date.to_string();
        let rows = self
            .conn
            .query(
                "SELECT partition_name, status FROM create_tasks_partitions_range($1::text::date, $2::text::date)",
                &[&start_param, &end_param],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to create partitions range: {}", e))
            })?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String = row
                .col("partition_name")
                .map_err(|e| CelersError::Other(format!("Failed to read partition_name: {}", e)))?;
            let status: String = row
                .col("status")
                .map_err(|e| CelersError::Other(format!("Failed to read status: {}", e)))?;
            results.push((name, status));
        }

        Ok(results)
    }

    /// Drop a partition for a specific date
    ///
    /// **WARNING**: This permanently deletes all tasks in the partition!
    /// Use this for archiving old partitions after backing up the data.
    ///
    /// # Arguments
    /// * `partition_date` - Any date within the month to drop partition for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::NaiveDate;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Drop partition for January 2024 (after backing up!)
    /// let old_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// broker.drop_partition(old_date).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn drop_partition(&self, partition_date: chrono::NaiveDate) -> Result<String> {
        let date_param = partition_date.to_string();
        let rows = self
            .conn
            .query(
                "SELECT drop_tasks_partition($1::text::date)",
                &[&date_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to drop partition: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to drop partition: no rows returned".to_string())
        })?;

        row.col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read drop partition result: {}", e)))
    }

    /// List all task partitions with their statistics
    ///
    /// Returns information about each partition including row count and size.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let partitions = broker.list_partitions().await?;
    /// for partition in partitions {
    ///     println!("{}: {} rows, {} bytes",
    ///         partition.partition_name,
    ///         partition.row_count,
    ///         partition.size_bytes);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_partitions(&self) -> Result<Vec<PartitionInfo>> {
        let rows = self
            .conn
            .query(
                "SELECT partition_name, partition_start, partition_end, row_count, size_bytes
             FROM list_tasks_partitions()",
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list partitions: {}", e)))?;

        let mut partitions = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String = row
                .col("partition_name")
                .map_err(|e| CelersError::Other(format!("Failed to read partition_name: {}", e)))?;
            // `chrono::NaiveDate` DOES have `oxisql_core::FromValue` on the
            // read side (unlike the write side used above for parameters).
            let start: chrono::NaiveDate = row.col("partition_start").map_err(|e| {
                CelersError::Other(format!("Failed to read partition_start: {}", e))
            })?;
            let end: chrono::NaiveDate = row
                .col("partition_end")
                .map_err(|e| CelersError::Other(format!("Failed to read partition_end: {}", e)))?;
            let row_count: i64 = row
                .col("row_count")
                .map_err(|e| CelersError::Other(format!("Failed to read row_count: {}", e)))?;
            let size_bytes: i64 = row
                .col("size_bytes")
                .map_err(|e| CelersError::Other(format!("Failed to read size_bytes: {}", e)))?;

            partitions.push(PartitionInfo {
                partition_name: name,
                partition_start: DateTime::from_naive_utc_and_offset(
                    start
                        .and_hms_opt(0, 0, 0)
                        .expect("midnight 00:00:00 is always valid"),
                    Utc,
                ),
                partition_end: DateTime::from_naive_utc_and_offset(
                    end.and_hms_opt(0, 0, 0)
                        .expect("midnight 00:00:00 is always valid"),
                    Utc,
                ),
                row_count,
                size_bytes,
            });
        }

        Ok(partitions)
    }

    /// Maintain partitions by creating future partitions automatically
    ///
    /// This creates partitions for the current month plus `months_ahead` months.
    /// Should be called periodically (e.g., daily or weekly) to ensure partitions
    /// exist before tasks are enqueued.
    ///
    /// # Arguments
    /// * `months_ahead` - Number of months ahead to create partitions for (default: 3)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Create partitions for current month + 3 months ahead
    /// broker.maintain_partitions(3).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn maintain_partitions(&self, months_ahead: i32) -> Result<String> {
        let rows = self
            .conn
            .query("SELECT maintain_tasks_partitions($1)", &[&months_ahead])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to maintain partitions: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to maintain partitions: no rows returned".to_string())
        })?;

        row.col_idx(0).map_err(|e| {
            CelersError::Other(format!("Failed to read maintain partitions result: {}", e))
        })
    }

    /// Get the partition name for a specific date
    ///
    /// Useful for understanding which partition a task will be stored in.
    ///
    /// # Arguments
    /// * `task_date` - Date to get partition name for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::Utc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let partition_name = broker.get_partition_name(Utc::now().naive_utc().date()).await?;
    /// println!("Current partition: {}", partition_name);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_partition_name(&self, task_date: chrono::NaiveDate) -> Result<String> {
        let date_param = task_date.to_string();
        let rows = self
            .conn
            .query(
                "SELECT get_tasks_partition_name($1::text::date)",
                &[&date_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get partition name: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get partition name: no rows returned".to_string())
        })?;

        row.col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read partition name: {}", e)))
    }

    /// Detach a partition for archiving without deleting data
    ///
    /// This detaches the partition from the main table but doesn't delete it.
    /// Useful for archiving old data to separate storage before dropping.
    ///
    /// # Arguments
    /// * `partition_date` - Any date within the month to detach partition for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::NaiveDate;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Detach partition for archiving
    /// let old_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// broker.detach_partition(old_date).await?;
    /// // Now you can pg_dump the detached table and then drop it
    /// # Ok(())
    /// # }
    /// ```
    pub async fn detach_partition(&self, partition_date: chrono::NaiveDate) -> Result<String> {
        let partition_name = self.get_partition_name(partition_date).await?;

        let query_str = format!(
            "ALTER TABLE celers_tasks DETACH PARTITION {}",
            partition_name
        );
        self.conn
            .execute(&query_str, &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to detach partition: {}", e)))?;

        Ok(format!("Detached partition: {}", partition_name))
    }
}
