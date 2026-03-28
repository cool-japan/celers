pub mod patterns {
    //! Quick start guide for common patterns
    //!
    //! # Common Patterns
    //!
    //! ## Creating a Simple Task
    //! ```rust,ignore
    //! use celers::prelude::*;
    //!
    //! #[derive(Serialize, Deserialize)]
    //! struct Args { x: i32, y: i32 }
    //!
    //! #[celers::task]
    //! async fn add(args: Args) -> TaskResult<i32> {
    //!     Ok(args.x + args.y)
    //! }
    //! ```
    //!
    //! ## Creating a Worker
    //! ```rust,ignore
    //! use celers::prelude::*;
    //!
    //! let broker = create_broker_from_env().await?;
    //! let worker = WorkerConfigBuilder::new()
    //!     .concurrency(4)
    //!     .prefetch_count(10)
    //!     .build(broker)?;
    //! worker.start().await?;
    //! ```
    //!
    //! ## Sending Tasks
    //! ```rust,ignore
    //! let task = my_task::new(Args { x: 1, y: 2 });
    //! broker.enqueue(task).await?;
    //! ```
    //!
    //! ## Creating Workflows
    //! ```rust,ignore
    //! // Chain: Sequential execution
    //! let chain = Chain::new()
    //!     .then("task1", vec![])
    //!     .then("task2", vec![]);
    //!
    //! // Group: Parallel execution
    //! let group = Group::new()
    //!     .add("task1", vec![])
    //!     .add("task2", vec![]);
    //!
    //! // Chord: Parallel + callback
    //! let chord = Chord::new(group, callback);
    //! ```
}

pub mod config_examples {
    //! Configuration examples and snippets
    //!
    //! # Configuration Examples
    //!
    //! ## Environment Variables
    //! ```bash
    //! export CELERS_BROKER_TYPE=redis
    //! export CELERS_BROKER_URL=redis://localhost:6379
    //! export CELERS_BROKER_QUEUE=celery
    //! ```
    //!
    //! ## Worker Presets
    //! ```rust,ignore
    //! use celers::presets::*;
    //!
    //! // Production config
    //! let config = production_config();
    //!
    //! // High throughput
    //! let config = high_throughput_config();
    //!
    //! // Low latency
    //! let config = low_latency_config();
    //!
    //! // Memory constrained
    //! let config = memory_constrained_config();
    //! ```
}

pub mod troubleshooting {
    //! Troubleshooting guide
    //!
    //! # Troubleshooting Guide
    //!
    //! ## Common Issues
    //!
    //! ### Connection Refused
    //! - Check broker is running: `redis-cli ping` or `psql`
    //! - Verify connection URL format
    //! - Check firewall rules
    //!
    //! ### Tasks Not Processing
    //! - Verify worker is running
    //! - Check queue name matches
    //! - Inspect worker logs
    //!
    //! ### High Memory Usage
    //! - Reduce prefetch_count
    //! - Implement chunked processing
    //! - Monitor task memory usage
    //!
    //! ### Slow Task Execution
    //! - Increase worker concurrency
    //! - Profile task execution time
    //! - Optimize database queries
}
