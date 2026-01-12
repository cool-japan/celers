//! Advanced workflow patterns: ScatterGather, Pipeline, FanOut, FanIn

use celers_canvas::{FanIn, FanOut, Pipeline, ScatterGather, Signature, StreamingMapReduce};

fn main() {
    println!("=== Advanced Workflow Patterns ===\n");

    // Example 1: ScatterGather Pattern
    println!("1. ScatterGather Pattern:");
    let scatter_gather = ScatterGather::new(
        Signature::new("broadcast_query".to_string()),
        vec![
            Signature::new("search_db1".to_string()),
            Signature::new("search_db2".to_string()),
            Signature::new("search_db3".to_string()),
        ],
        Signature::new("merge_results".to_string()),
    );
    println!("   Workers: {}", scatter_gather.workers.len());
    println!();

    // Example 2: Pipeline Pattern
    println!("2. Pipeline Pattern:");
    let pipeline = Pipeline::new()
        .stage(Signature::new("stage1_parse".to_string()))
        .stage(Signature::new("stage2_validate".to_string()))
        .stage(Signature::new("stage3_transform".to_string()))
        .stage(Signature::new("stage4_store".to_string()))
        .with_buffer_size(1000);
    println!("   Stages: {}", pipeline.stages.len());
    println!("   Buffer size: {:?}", pipeline.buffer_size);
    println!();

    // Example 3: FanOut Pattern
    println!("3. FanOut Pattern:");
    let fanout = FanOut::new(Signature::new("generate_data".to_string()))
        .consumer(Signature::new("save_to_db".to_string()))
        .consumer(Signature::new("send_to_queue".to_string()))
        .consumer(Signature::new("log_event".to_string()));
    println!("   Source: {}", fanout.source.task);
    println!("   Consumers: {}", fanout.consumers.len());
    println!();

    // Example 4: FanIn Pattern
    println!("4. FanIn Pattern:");
    let fanin = FanIn::new(Signature::new("aggregate_observability".to_string()))
        .source(Signature::new("collect_metrics".to_string()))
        .source(Signature::new("collect_logs".to_string()))
        .source(Signature::new("collect_traces".to_string()));
    println!("   Sources: {}", fanin.sources.len());
    println!("   Aggregator: {}", fanin.aggregator.task);
    println!();

    // Example 5: Streaming MapReduce
    println!("5. Streaming MapReduce:");
    let streaming_mr = StreamingMapReduce::new(
        Signature::new("stream_processor".to_string()),
        Signature::new("reduce_stream".to_string()),
    )
    .with_chunk_size(100)
    .with_buffer_size(1000)
    .with_backpressure(true);
    println!("   Chunk size: {}", streaming_mr.chunk_size);
    println!("   Buffer size: {}", streaming_mr.buffer_size);
    println!("   Backpressure: {}", streaming_mr.backpressure);
    println!();

    // Example 6: Complex Nested Pattern
    println!("6. Complex Pattern - ScatterGather with Pipelines:");
    let complex = ScatterGather::new(
        Signature::new("distribute_workload".to_string()),
        vec![
            Signature::new("pipeline_worker_1".to_string()),
            Signature::new("pipeline_worker_2".to_string()),
            Signature::new("pipeline_worker_3".to_string()),
        ],
        Signature::new("collect_results".to_string()),
    );
    println!("   This pattern distributes work to multiple pipeline workers");
    println!("   Each worker processes data through multiple stages");
    println!("   Results are gathered and merged at the end");
    println!("   Total workers: {}", complex.workers.len());
    println!();

    println!("=== All advanced patterns demonstrated ===");
}
