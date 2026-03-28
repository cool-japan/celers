//! Partition management methods for PostgreSQL table partitioning

use celers_core::{CelersError, Result};
use chrono::{DateTime, Utc};
use sqlx::Row;

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
        let result: String = sqlx::query_scalar("SELECT create_tasks_partition($1)")
            .bind(partition_date)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to create partition: {}", e)))?;

        Ok(result)
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
        let rows =
            sqlx::query("SELECT partition_name, status FROM create_tasks_partitions_range($1, $2)")
                .bind(start_date)
                .bind(end_date)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to create partitions range: {}", e))
                })?;

        let results: Vec<(String, String)> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("partition_name");
                let status: String = row.get("status");
                (name, status)
            })
            .collect();

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
        let result: String = sqlx::query_scalar("SELECT drop_tasks_partition($1)")
            .bind(partition_date)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to drop partition: {}", e)))?;

        Ok(result)
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
        let rows = sqlx::query(
            "SELECT partition_name, partition_start, partition_end, row_count, size_bytes
             FROM list_tasks_partitions()",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list partitions: {}", e)))?;

        let partitions: Vec<PartitionInfo> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("partition_name");
                let start: chrono::NaiveDate = row.get("partition_start");
                let end: chrono::NaiveDate = row.get("partition_end");
                let row_count: i64 = row.get("row_count");
                let size_bytes: i64 = row.get("size_bytes");

                PartitionInfo {
                    partition_name: name,
                    partition_start: DateTime::from_naive_utc_and_offset(
                        start.and_hms_opt(0, 0, 0).unwrap(),
                        Utc,
                    ),
                    partition_end: DateTime::from_naive_utc_and_offset(
                        end.and_hms_opt(0, 0, 0).unwrap(),
                        Utc,
                    ),
                    row_count,
                    size_bytes,
                }
            })
            .collect();

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
        let result: String = sqlx::query_scalar("SELECT maintain_tasks_partitions($1)")
            .bind(months_ahead)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to maintain partitions: {}", e)))?;

        Ok(result)
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
        let result: String = sqlx::query_scalar("SELECT get_tasks_partition_name($1)")
            .bind(task_date)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get partition name: {}", e)))?;

        Ok(result)
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

        sqlx::query(&format!(
            "ALTER TABLE celers_tasks DETACH PARTITION {}",
            partition_name
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to detach partition: {}", e)))?;

        Ok(format!("Detached partition: {}", partition_name))
    }
}
