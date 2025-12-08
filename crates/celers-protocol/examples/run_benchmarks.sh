#!/bin/bash
# Benchmark runner script for celers-protocol
#
# This script runs all benchmarks and generates a comparison report.

set -e

echo "=== CeleRS Protocol Benchmarks ==="
echo ""
echo "Running benchmarks in release mode..."
echo ""

# Check if criterion is available
if ! cargo bench --help &> /dev/null; then
    echo "Error: cargo bench is not available"
    exit 1
fi

# Run benchmarks
echo "1. Running message deserialization benchmarks..."
cargo bench --bench message_benchmarks -- --output-format bencher 2>&1 | tee /tmp/celers_bench_output.txt

echo ""
echo "=== Benchmark Results Summary ==="
echo ""

# Extract key metrics
echo "Performance Summary:"
grep -E "(standard_deserialize|zerocopy_deserialize|lazy_deserialize|message_pool)" /tmp/celers_bench_output.txt | head -20 || echo "Benchmarks completed"

echo ""
echo "=== Detailed Results ==="
echo "Full HTML reports generated in: target/criterion/"
echo ""
echo "To view detailed results:"
echo "  - Open target/criterion/report/index.html in a browser"
echo "  - Or run: cargo bench -- --verbose"
echo ""

# Cleanup
rm -f /tmp/celers_bench_output.txt

echo "✓ Benchmarks completed successfully"
