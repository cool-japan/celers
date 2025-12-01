//! Image Processing Pipeline Example
//!
//! This example demonstrates a distributed image processing system using CeleRS.
//!
//! Features demonstrated:
//! - Batch image processing with task queues
//! - Multi-stage processing pipeline
//! - Canvas workflows (Chain, Group, Chord)
//! - Result aggregation and reporting
//! - Memory-efficient processing
//! - Error handling and retries
//!
//! # Architecture
//!
//! Pipeline stages:
//! 1. **Load**: Read image from storage
//! 2. **Resize**: Resize to multiple dimensions
//! 3. **Filter**: Apply filters (grayscale, blur, sharpen)
//! 4. **Compress**: Optimize file size
//! 5. **Save**: Store processed images
//! 6. **Report**: Generate processing statistics
//!
//! # Running
//!
//! ```bash
//! # Start Redis (required)
//! docker run -d -p 6379:6379 redis:latest
//!
//! # Run the example
//! cargo run --example image_processing
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, TaskRegistry};
use celers_macros::task;
use celers_worker::{Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{error, info};

/// Image metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageMetadata {
    id: String,
    path: String,
    width: u32,
    height: u32,
    format: String,
    size_bytes: u64,
}

/// Image processing request
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingRequest {
    image_id: String,
    operations: Vec<String>,
    target_sizes: Vec<(u32, u32)>,
}

/// Processing result
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingResult {
    image_id: String,
    original_size: u64,
    processed_variants: Vec<ImageVariant>,
    total_processing_time_ms: u64,
}

/// Image variant (different size/filter combination)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageVariant {
    size: (u32, u32),
    filters: Vec<String>,
    size_bytes: u64,
    path: String,
}

/// Arguments for resize task
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResizeArgs {
    metadata: ImageMetadata,
    target_size: (u32, u32),
}

/// Arguments for filter task
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilterArgs {
    variant: ImageVariant,
    filter: String,
}

/// Arguments for compress task
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompressArgs {
    variant: ImageVariant,
    quality: u8,
}

/// Arguments for report task
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportArgs {
    image_id: String,
    variants: Vec<ImageVariant>,
}

/// Shared state for processing results
#[allow(dead_code)]
type ProcessingResults = Arc<Mutex<HashMap<String, ProcessingResult>>>;

/// Task: Load image from storage
#[task]
async fn load_image(request: ProcessingRequest) -> celers_core::Result<ImageMetadata> {
    info!("Loading image: {}", request.image_id);

    // Simulate loading delay
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Simulate image metadata
    let metadata = ImageMetadata {
        id: request.image_id.clone(),
        path: format!("/images/{}.jpg", request.image_id),
        width: 3840,
        height: 2160,
        format: "JPEG".to_string(),
        size_bytes: 5_242_880, // 5MB
    };

    info!(
        "Loaded: {} ({}x{}, {} bytes)",
        metadata.id, metadata.width, metadata.height, metadata.size_bytes
    );

    Ok(metadata)
}

/// Task: Resize image
#[task]
async fn resize_image(args: ResizeArgs) -> celers_core::Result<ImageVariant> {
    info!(
        "Resizing {} from {}x{} to {}x{}",
        args.metadata.id,
        args.metadata.width,
        args.metadata.height,
        args.target_size.0,
        args.target_size.1
    );

    // Simulate CPU-intensive resize operation
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Calculate scaled size (proportional to resolution)
    let scale_factor = (args.target_size.0 * args.target_size.1) as f64
        / (args.metadata.width * args.metadata.height) as f64;
    let new_size = (args.metadata.size_bytes as f64 * scale_factor) as u64;

    let variant = ImageVariant {
        size: args.target_size,
        filters: vec![],
        size_bytes: new_size,
        path: format!(
            "/images/{}_{}x{}.jpg",
            args.metadata.id, args.target_size.0, args.target_size.1
        ),
    };

    info!(
        "Resized to {}x{}: {} bytes",
        variant.size.0, variant.size.1, variant.size_bytes
    );

    Ok(variant)
}

/// Task: Apply image filter
#[task]
async fn apply_filter(args: FilterArgs) -> celers_core::Result<ImageVariant> {
    info!(
        "Applying filter '{}' to {:?}",
        args.filter, args.variant.size
    );

    // Simulate filter processing
    tokio::time::sleep(Duration::from_millis(80)).await;

    let mut variant = args.variant;
    variant.filters.push(args.filter.clone());

    // Grayscale reduces size slightly
    if args.filter == "grayscale" {
        variant.size_bytes = (variant.size_bytes as f64 * 0.9) as u64;
    }

    info!(
        "Applied filter '{}': filters={:?}",
        args.filter, variant.filters
    );

    Ok(variant)
}

/// Task: Compress image
#[task]
async fn compress_image(args: CompressArgs) -> celers_core::Result<ImageVariant> {
    info!(
        "Compressing {:?} (quality: {}%)",
        args.variant.size, args.quality
    );

    // Simulate compression
    tokio::time::sleep(Duration::from_millis(60)).await;

    let mut variant = args.variant;

    // Reduce size based on quality
    let compression_ratio = args.quality as f64 / 100.0;
    let original_size = variant.size_bytes;
    variant.size_bytes = (variant.size_bytes as f64 * compression_ratio) as u64;

    info!(
        "Compressed to {} bytes ({}% reduction)",
        variant.size_bytes,
        100 - (variant.size_bytes * 100 / original_size)
    );

    Ok(variant)
}

/// Task: Save processed image
#[task]
async fn save_image(variant: ImageVariant) -> celers_core::Result<()> {
    info!("Saving: {} ({} bytes)", variant.path, variant.size_bytes);

    // Simulate save operation
    sleep(Duration::from_millis(30)).await;

    info!("Saved successfully: {}", variant.path);

    Ok(())
}

/// Task: Generate processing report
#[task]
async fn generate_report(args: ReportArgs) -> celers_core::Result<()> {
    info!("Generating report for: {}", args.image_id);

    let total_size: u64 = args.variants.iter().map(|v| v.size_bytes).sum();

    info!(
        "Report: {} variants generated, total size: {} bytes",
        args.variants.len(),
        total_size
    );

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("=== CeleRS Image Processing Pipeline Example ===");

    // Create Redis broker
    let broker = RedisBroker::new("redis://127.0.0.1:6379", "image_processing")?;

    info!("Connected to Redis broker");

    // Create task registry
    let registry = TaskRegistry::new();

    // Register tasks using macro-generated structs
    registry.register(LoadImageTask).await;
    registry.register(ResizeImageTask).await;
    registry.register(ApplyFilterTask).await;
    registry.register(CompressImageTask).await;
    registry.register(SaveImageTask).await;
    registry.register(GenerateReportTask).await;

    info!("Registered tasks: {:?}", registry.list_tasks().await);

    // Enqueue image processing requests
    info!("Enqueueing image processing jobs...");

    let images = vec!["photo_001", "photo_002", "photo_003"];
    let target_sizes = vec![(1920, 1080), (1280, 720), (640, 480)];

    for image_id in &images {
        let request = ProcessingRequest {
            image_id: image_id.to_string(),
            operations: vec!["grayscale".to_string(), "sharpen".to_string()],
            target_sizes: target_sizes.clone(),
        };

        let args = serde_json::to_vec(&request)?;
        let task = SerializedTask::new("load_image".to_string(), args);

        broker.enqueue(task).await?;
        info!("Enqueued processing job for: {}", image_id);
    }

    // Create and start worker
    let config = WorkerConfig::builder()
        .concurrency(8) // High concurrency for parallel processing
        .max_retries(3)
        .default_timeout_secs(60)
        .poll_interval_ms(200)
        .enable_batch_dequeue(true)
        .batch_size(5)
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;

    info!(
        "Starting worker with {} concurrent tasks (batch mode)...",
        config.concurrency
    );

    let worker = Worker::new(broker, registry, config);

    // Run worker for limited time (for demo purposes)
    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            error!("Worker error: {}", e);
        }
    });

    // Let it run for 15 seconds
    info!("Processing images for 15 seconds...");
    sleep(Duration::from_secs(15)).await;

    info!("\nImage processing pipeline example completed!");
    worker_handle.abort();

    Ok(())
}
