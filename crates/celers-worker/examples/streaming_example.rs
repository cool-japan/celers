//! Task Result Streaming Example
//!
//! This example demonstrates how to use the streaming module to handle
//! large task results without loading everything into memory at once.

use celers_worker::streaming::{ResultStreamer, StreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Task Result Streaming Example ===\n");

    // Create a streaming configuration
    let config = StreamConfig::new()
        .with_chunk_size(1024) // 1 KB chunks
        .with_max_buffer_size(10_240) // 10 KB buffer
        .with_stream_threshold(5_120) // Stream files > 5 KB
        .with_channel_buffer(50) // Buffer up to 50 chunks
        .with_max_memory_per_task(102_400); // 100 KB per task

    println!("Stream Configuration:");
    println!("  Chunk size: {} bytes", config.chunk_size);
    println!("  Max buffer: {} bytes", config.max_buffer_size);
    println!("  Stream threshold: {} bytes", config.stream_threshold);
    println!("  Channel buffer: {} chunks", config.channel_buffer);
    println!(
        "  Max memory per task: {} bytes\n",
        config.max_memory_per_task
    );

    // Validate configuration
    config.validate()?;
    println!("Configuration is valid\n");

    // Create a result streamer
    let streamer = ResultStreamer::new(config.clone());

    // Example 1: Small data (won't be streamed)
    println!("=== Example 1: Small Data ===\n");
    let small_data = vec![1u8; 1000]; // 1 KB
    println!("Data size: {} bytes", small_data.len());
    println!(
        "Should stream: {}",
        streamer.should_stream(small_data.len())
    );
    println!("This data will be sent as a single chunk\n");

    // Example 2: Large data (will be streamed)
    println!("=== Example 2: Large Data (Streaming) ===\n");
    let large_data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect(); // 10 KB
    println!("Data size: {} bytes", large_data.len());
    println!(
        "Should stream: {}",
        streamer.should_stream(large_data.len())
    );

    let num_chunks = config.calculate_chunks(large_data.len());
    println!("Will be split into {} chunks\n", num_chunks);

    // Split into chunks
    let chunks = streamer.split_into_chunks(large_data.clone());
    println!("Chunks created:");
    for (i, chunk) in chunks.iter().enumerate() {
        println!(
            "  Chunk {}: {} bytes, sequence: {}, is_last: {}",
            i,
            chunk.size(),
            chunk.sequence,
            chunk.is_last
        );
        if let Some(checksum) = chunk.checksum {
            println!(
                "           checksum: 0x{:08x}, valid: {}",
                checksum,
                chunk.verify_checksum()
            );
        }
    }
    println!();

    // Example 3: Sender and Receiver
    println!("=== Example 3: Sender/Receiver Communication ===\n");

    // Create a new streamer for this example
    let streamer3 = ResultStreamer::new(config.clone());
    let (mut sender, mut receiver) = streamer3.create_sender();

    // Data to send
    let test_data: Vec<u8> = (0..5_000).map(|i| (i % 256) as u8).collect();
    println!("Sending {} bytes of data...", test_data.len());

    let chunk_size = config.chunk_size;
    // Send data in the background
    let send_handle = tokio::spawn(async move {
        let total_chunks = test_data.len().div_ceil(chunk_size);
        match sender.send_data(test_data, total_chunks as u64).await {
            Ok(_) => {
                println!("All chunks sent successfully");
                sender.close();
            }
            Err(e) => eprintln!("Error sending data: {}", e),
        }
    });

    // Receive all chunks
    let received_data = receiver.recv_all().await?;
    println!("Received {} bytes of data", received_data.len());
    println!("Chunks received: {}", receiver.received_count());
    println!("Reception complete: {}\n", receiver.is_complete());

    // Wait for sender to finish
    send_handle.await?;

    // Verify data integrity
    let original_data: Vec<u8> = (0..5_000).map(|i| (i % 256) as u8).collect();
    if received_data == original_data {
        println!("Data integrity verified!");
    } else {
        println!("Data integrity check FAILED!");
    }

    // Get streaming statistics
    let stats = streamer3.get_stats().await;
    println!("\nStreaming Statistics:");
    println!("  Bytes streamed: {}", stats.bytes_streamed);
    println!("  Chunks sent: {}", stats.chunks_sent);
    println!("  Chunks received: {}", stats.chunks_received);
    println!("  Peak buffer usage: {} bytes", stats.peak_buffer_usage);
    println!(
        "  Buffer utilization: {:.1}%",
        stats.buffer_utilization(config.max_buffer_size)
    );
    println!("  Average chunk size: {:.1} bytes", stats.avg_chunk_size());
    println!("  Backpressure events: {}", stats.backpressure_count);

    // Example 4: Memory limit enforcement
    println!("\n=== Example 4: Memory Limit Enforcement ===\n");
    let strict_config = StreamConfig::new()
        .with_chunk_size(100)
        .with_max_memory_per_task(500); // Very small limit

    let strict_streamer = ResultStreamer::new(strict_config);
    let (mut strict_sender, _strict_receiver) = strict_streamer.create_sender();

    // Try to send data that exceeds memory limit
    let large_chunk_data = vec![1u8; 1000];
    let large_chunk = celers_worker::streaming::DataChunk::new(0, 1, large_chunk_data, true);

    println!("Attempting to send chunk larger than memory limit...");
    match strict_sender.send(large_chunk).await {
        Ok(_) => println!("Chunk sent (unexpected)"),
        Err(e) => println!("Chunk rejected: {}", e),
    }

    println!("\n=== Example Complete ===");

    Ok(())
}
