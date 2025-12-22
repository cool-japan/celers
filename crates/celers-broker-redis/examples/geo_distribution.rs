//! Geo-distribution example
//!
//! Demonstrates:
//! - Multi-region replication
//! - Regional read routing
//! - Conflict resolution
//! - Region statistics
//!
//! Run with: cargo run --example geo_distribution
//! Requires: Multiple Redis instances or modify for single instance testing

use celers_broker_redis::geo::{
    ConflictResolution, GeoReplicationManager, Region, RegionId, ReplicationConfig,
    RoutingStrategy, SyncMode,
};
use celers_core::{SerializedTask, TaskMetadata};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Geo-Distribution Example ===\n");

    // Example 1: Region Setup
    println!("1. Region Configuration");
    region_setup_example().await?;

    // Example 2: Replication Strategies
    println!("\n2. Replication Strategies");
    replication_strategies_example().await?;

    // Example 3: Conflict Resolution
    println!("\n3. Conflict Resolution");
    conflict_resolution_example().await?;

    // Example 4: Read Routing
    println!("\n4. Regional Read Routing");
    read_routing_example().await?;

    println!("\n=== All geo-distribution examples completed! ===");
    Ok(())
}

async fn region_setup_example() -> Result<(), Box<dyn std::error::Error>> {
    // Define geographic regions
    let us_east = Region::new(
        RegionId::new("us-east-1"),
        "US East (N. Virginia)",
        "redis://localhost:6379", // In production: use actual region URL
        38.9072,                  // Latitude
        -77.0369,                 // Longitude
        true,                     // Primary region
    );

    let eu_west = Region::new(
        RegionId::new("eu-west-1"),
        "EU West (Ireland)",
        "redis://localhost:6379", // In production: use different port/host
        53.3498,
        -6.2603,
        false,
    );

    let ap_southeast = Region::new(
        RegionId::new("ap-southeast-1"),
        "Asia Pacific (Singapore)",
        "redis://localhost:6379",
        1.3521,
        103.8198,
        false,
    );

    println!("  Configured regions:");
    println!(
        "    {} - {} ({}, {})",
        us_east.id(),
        us_east.name(),
        us_east.latitude(),
        us_east.longitude()
    );
    println!(
        "    {} - {} ({}, {})",
        eu_west.id(),
        eu_west.name(),
        eu_west.latitude(),
        eu_west.longitude()
    );
    println!(
        "    {} - {} ({}, {})",
        ap_southeast.id(),
        ap_southeast.name(),
        ap_southeast.latitude(),
        ap_southeast.longitude()
    );

    // Calculate distances
    let distance_us_eu = us_east.distance_to(&eu_west);
    let distance_us_ap = us_east.distance_to(&ap_southeast);
    let distance_eu_ap = eu_west.distance_to(&ap_southeast);

    println!("\n  Geographic distances:");
    println!("    US East <-> EU West: {:.0} km", distance_us_eu);
    println!("    US East <-> AP Southeast: {:.0} km", distance_us_ap);
    println!("    EU West <-> AP Southeast: {:.0} km", distance_eu_ap);

    Ok(())
}

async fn replication_strategies_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Replication strategies:\n");

    // Strategy 1: Async Replication (fastest, eventual consistency)
    println!("  1. Async Replication");
    println!("     - Writes to primary, async replication to secondaries");
    println!("     - Lowest latency, eventual consistency");
    println!("     - Best for: high-throughput, non-critical data");

    let async_config = ReplicationConfig::builder()
        .sync_mode(SyncMode::AsyncReplication)
        .replication_timeout_ms(1000)
        .conflict_resolution(ConflictResolution::LastWriteWins)
        .build();

    let async_manager = GeoReplicationManager::new(RegionId::new("us-east-1"), async_config);

    println!("     Config: {:?}", async_manager.config().sync_mode());

    // Strategy 2: Quorum Sync (balanced)
    println!("\n  2. Quorum Sync");
    println!("     - Writes to quorum of regions before returning");
    println!("     - Balanced latency and consistency");
    println!("     - Best for: most production workloads");

    let quorum_config = ReplicationConfig::builder()
        .sync_mode(SyncMode::QuorumSync)
        .replication_timeout_ms(2000)
        .conflict_resolution(ConflictResolution::PrimaryWins)
        .build();

    let quorum_manager = GeoReplicationManager::new(RegionId::new("us-east-1"), quorum_config);

    println!("     Config: {:?}", quorum_manager.config().sync_mode());

    // Strategy 3: Full Sync (strongest consistency)
    println!("\n  3. Full Sync");
    println!("     - Writes to all regions before returning");
    println!("     - Highest latency, strongest consistency");
    println!("     - Best for: critical data, compliance requirements");

    let full_config = ReplicationConfig::builder()
        .sync_mode(SyncMode::FullSync)
        .replication_timeout_ms(5000)
        .conflict_resolution(ConflictResolution::Manual)
        .build();

    let full_manager = GeoReplicationManager::new(RegionId::new("us-east-1"), full_config);

    println!("     Config: {:?}", full_manager.config().sync_mode());

    Ok(())
}

async fn conflict_resolution_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplicationConfig::default();
    let _manager = GeoReplicationManager::new(RegionId::new("us-east-1"), config);

    println!("  Conflict resolution strategies:\n");

    // Create conflicting tasks
    let task1 = create_task("order_processing", 5, vec![1, 2, 3]);
    let task2 = create_task("order_processing", 8, vec![4, 5, 6]);
    let task3 = create_task("order_processing", 3, vec![7, 8, 9]);

    let conflicts = vec![
        (RegionId::new("us-east-1"), task1.clone()),
        (RegionId::new("eu-west-1"), task2.clone()),
        (RegionId::new("ap-southeast-1"), task3.clone()),
    ];

    // Strategy 1: Last Write Wins
    println!("  1. Last Write Wins:");
    let config1 = ReplicationConfig::builder()
        .conflict_resolution(ConflictResolution::LastWriteWins)
        .build();
    let manager1 = GeoReplicationManager::new(RegionId::new("us-east-1"), config1);

    if let Some(resolved) = manager1.resolve_conflict(conflicts.clone()) {
        println!("     Resolved to task with ID: {}", resolved.metadata.id);
        println!("     Priority: {}", resolved.metadata.priority);
    }

    // Strategy 2: Primary Wins
    println!("\n  2. Primary Wins:");
    let config2 = ReplicationConfig::builder()
        .conflict_resolution(ConflictResolution::PrimaryWins)
        .build();
    let manager2 = GeoReplicationManager::new(RegionId::new("us-east-1"), config2);

    if let Some(resolved) = manager2.resolve_conflict(conflicts.clone()) {
        println!("     Resolved to task from primary region");
        println!("     Priority: {}", resolved.metadata.priority);
    }

    // Strategy 3: Highest Priority Wins
    println!("\n  3. Highest Priority Wins:");
    let config3 = ReplicationConfig::builder()
        .conflict_resolution(ConflictResolution::HighestPriorityWins)
        .build();
    let manager3 = GeoReplicationManager::new(RegionId::new("us-east-1"), config3);

    if let Some(resolved) = manager3.resolve_conflict(conflicts.clone()) {
        println!(
            "     Resolved to task with highest priority: {}",
            resolved.metadata.priority
        );
    }

    Ok(())
}

async fn read_routing_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Read routing strategies:\n");

    // Strategy 1: Nearest Region
    println!("  1. Nearest Region Routing");
    println!("     - Routes reads to geographically closest region");
    println!("     - Best for: latency-sensitive applications");
    println!("     - Strategy: {}", RoutingStrategy::NearestRegion);

    // Strategy 2: Lowest Latency
    println!("\n  2. Lowest Latency Routing");
    println!("     - Routes reads to region with lowest replication lag");
    println!("     - Best for: consistent read performance");
    println!("     - Strategy: {}", RoutingStrategy::LowestLatency);

    // Strategy 3: Round Robin
    println!("\n  3. Round Robin Routing");
    println!("     - Distributes reads evenly across all regions");
    println!("     - Best for: load balancing");
    println!("     - Strategy: {}", RoutingStrategy::RoundRobin);

    // Strategy 4: Random
    println!("\n  4. Random Routing");
    println!("     - Randomly selects a region for each read");
    println!("     - Best for: testing, development");
    println!("     - Strategy: {}", RoutingStrategy::Random);

    // Example: Client location-based routing
    println!("\n  Example client locations:");
    let locations = vec![
        ("New York", 40.7128, -74.0060),
        ("London", 51.5074, -0.1278),
        ("Singapore", 1.3521, 103.8198),
    ];

    for (city, lat, lon) in locations {
        println!("    Client in {}: ({}, {})", city, lat, lon);
        println!("      -> Would route to nearest region based on distance");
    }

    Ok(())
}

fn create_task(name: &str, priority: i32, payload: Vec<u8>) -> SerializedTask {
    let mut metadata = TaskMetadata::new(name.to_string());
    metadata.priority = priority;
    SerializedTask { metadata, payload }
}
