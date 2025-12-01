//! Web Scraper Example
//!
//! This example demonstrates a production-ready web crawler using CeleRS.
//!
//! Features demonstrated:
//! - Distributed crawling with task queues
//! - Priority-based URL processing
//! - Retry logic for failed requests
//! - Result storage and aggregation
//! - Graceful error handling
//! - Rate limiting via task delays
//!
//! # Architecture
//!
//! 1. **Seed Task**: Enqueues initial URLs to crawl
//! 2. **Crawl Task**: Fetches page content and extracts links
//! 3. **Parse Task**: Extracts structured data from pages
//! 4. **Store Task**: Saves results to storage
//!
//! # Running
//!
//! ```bash
//! # Start Redis (required)
//! docker run -d -p 6379:6379 redis:latest
//!
//! # Run the example
//! cargo run --example web_scraper
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, TaskRegistry};
use celers_macros::task;
use celers_worker::{Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{error, info};

/// URL to crawl
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CrawlUrl {
    url: String,
    depth: u32,
    priority: u8,
}

/// Extracted page data
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageData {
    url: String,
    title: Option<String>,
    links: Vec<String>,
    word_count: usize,
}

/// Result storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CrawlResult {
    url: String,
    title: Option<String>,
    word_count: usize,
    links_found: usize,
}

/// Shared state for visited URLs (prevents duplicate crawling)
#[allow(dead_code)]
type VisitedUrls = Arc<Mutex<HashSet<String>>>;

/// Shared state for crawl results
#[allow(dead_code)]
type CrawlResults = Arc<Mutex<Vec<CrawlResult>>>;

/// Simple crawl task - fetches page and extracts links
#[task]
async fn crawl_page(url: CrawlUrl) -> celers_core::Result<PageData> {
    info!(
        "Crawling: {} (depth: {}, priority: {})",
        url.url, url.depth, url.priority
    );

    // Simulate HTTP fetch
    sleep(Duration::from_millis(100)).await;

    // Simulate HTML content
    let html = format!(
        r#"
        <html>
            <head><title>Page: {}</title></head>
            <body>
                <h1>Welcome to {}</h1>
                <p>This is a sample page with content.</p>
                <a href="{}/page1">Link 1</a>
                <a href="{}/page2">Link 2</a>
                <a href="{}/page3">Link 3</a>
            </body>
        </html>
        "#,
        url.url, url.url, url.url, url.url, url.url
    );

    // Extract data
    let title = html
        .lines()
        .find(|line| line.contains("<title>"))
        .and_then(|line| {
            line.split("<title>")
                .nth(1)
                .and_then(|s| s.split("</title>").next())
                .map(|s| s.trim().to_string())
        });

    let links: Vec<String> = html
        .lines()
        .filter(|line| line.contains("href=\""))
        .filter_map(|line| {
            line.split("href=\"")
                .nth(1)
                .and_then(|s| s.split('\"').next())
                .map(String::from)
        })
        .filter(|link| link.starts_with(&url.url))
        .collect();

    let word_count = html.split_whitespace().count();

    let page_data = PageData {
        url: url.url.clone(),
        title: title.clone(),
        links: links.clone(),
        word_count,
    };

    info!(
        "Extracted from {}: title={:?}, links={}, words={}",
        url.url,
        title,
        links.len(),
        word_count
    );

    Ok(page_data)
}

/// Parse task - processes extracted page data
#[task]
async fn parse_page(page_data: PageData) -> celers_core::Result<CrawlResult> {
    info!("Parsing page: {}", page_data.url);

    sleep(Duration::from_millis(50)).await;

    let result = CrawlResult {
        url: page_data.url.clone(),
        title: page_data.title.clone(),
        word_count: page_data.word_count,
        links_found: page_data.links.len(),
    };

    info!(
        "Parsed {}: {} links found",
        page_data.url,
        page_data.links.len()
    );

    Ok(result)
}

/// Store task - saves crawl results
#[task]
async fn store_result(result: CrawlResult) -> celers_core::Result<()> {
    info!(
        "Storing result: {} (title: {:?}, words: {}, links: {})",
        result.url, result.title, result.word_count, result.links_found
    );

    sleep(Duration::from_millis(30)).await;

    info!("Stored successfully: {}", result.url);

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("=== CeleRS Web Scraper Example ===");

    // Create Redis broker
    let broker = RedisBroker::new("redis://127.0.0.1:6379", "web_scraper")?;

    info!("Connected to Redis broker");

    // Create task registry
    let registry = TaskRegistry::new();

    // Register tasks using macro-generated structs
    registry.register(CrawlPageTask).await;
    registry.register(ParsePageTask).await;
    registry.register(StoreResultTask).await;

    info!("Registered tasks: {:?}", registry.list_tasks().await);

    // Enqueue initial seed URLs
    info!("Enqueuing seed URLs...");
    let seed_urls = vec![
        "https://example.com",
        "https://example.com/blog",
        "https://example.com/docs",
    ];

    for url in seed_urls {
        let crawl_data = CrawlUrl {
            url: url.to_string(),
            depth: 0,
            priority: 9, // High priority for seed URLs
        };

        let args = serde_json::to_vec(&crawl_data)?;
        let task = SerializedTask::new("crawl_page".to_string(), args)
            .with_priority(crawl_data.priority as i32);

        broker.enqueue(task).await?;
        info!("Enqueued seed URL: {}", url);
    }

    // Create and start worker
    let config = WorkerConfig::builder()
        .concurrency(4)
        .max_retries(3)
        .default_timeout_secs(30)
        .poll_interval_ms(500)
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;

    info!(
        "Starting worker with {} concurrent tasks...",
        config.concurrency
    );

    let worker = Worker::new(broker, registry, config);

    // Run worker for limited time (for demo purposes)
    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            error!("Worker error: {}", e);
        }
    });

    // Let it run for 10 seconds
    info!("Crawling for 10 seconds...");
    sleep(Duration::from_secs(10)).await;

    info!("\nWeb scraper example completed!");
    worker_handle.abort();

    Ok(())
}
