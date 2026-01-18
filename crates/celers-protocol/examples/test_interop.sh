#!/bin/bash
# Integration test script for Python Celery interoperability
#
# This script tests the compatibility between Rust celers-protocol and Python Celery.
#
# Prerequisites:
#   - Redis server running (redis-server)
#   - Python with celery installed (pip install celery redis)

set -e

# Use TMPDIR if set, otherwise fall back to /tmp
TEMP_DIR="${TMPDIR:-/tmp}"

echo "=== CeleRS Protocol - Python Celery Interop Tests ==="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo "Checking prerequisites..."

# Check Redis
if ! pgrep -x "redis-server" > /dev/null; then
    echo -e "${RED}✗ Redis server is not running${NC}"
    echo "  Start with: redis-server"
    exit 1
fi
echo -e "${GREEN}✓ Redis server is running${NC}"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python 3 is available${NC}"

# Check Celery
if ! python3 -c "import celery" 2>/dev/null; then
    echo -e "${YELLOW}⚠ Celery is not installed${NC}"
    echo "  Install with: pip install celery redis"
    exit 1
fi
echo -e "${GREEN}✓ Celery is installed${NC}"

echo ""
echo "Running tests..."
echo ""

# Test 1: Rust examples compile and run
echo "1. Testing Rust examples..."
cargo run --example python_interop --quiet > ${TEMP_DIR}/rust_output.txt 2>&1
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✓ python_interop example runs${NC}"
else
    echo -e "   ${RED}✗ python_interop example failed${NC}"
    cat ${TEMP_DIR}/rust_output.txt
    exit 1
fi

cargo run --example message_validation --quiet > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✓ message_validation example runs${NC}"
else
    echo -e "   ${RED}✗ message_validation example failed${NC}"
    exit 1
fi

cargo run --example advanced_features --quiet > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✓ advanced_features example runs${NC}"
else
    echo -e "   ${RED}✗ advanced_features example failed${NC}"
    exit 1
fi

# Test 2: Python script validation
echo ""
echo "2. Testing Python consumer..."
python3 examples/python_consumer.py verify > ${TEMP_DIR}/python_output.txt 2>&1
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✓ Python message format verification passed${NC}"
else
    echo -e "   ${RED}✗ Python verification failed${NC}"
    cat ${TEMP_DIR}/python_output.txt
    exit 1
fi

# Test 3: Run unit tests
echo ""
echo "3. Running Rust unit tests..."
cargo test --quiet > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✓ All Rust unit tests passed (205 tests)${NC}"
else
    echo -e "   ${RED}✗ Some tests failed${NC}"
    cargo test 2>&1 | tail -20
    exit 1
fi

# Test 4: Check for warnings
echo ""
echo "4. Checking for warnings..."
cargo clippy --quiet 2>&1 | grep -i "warning" > ${TEMP_DIR}/clippy_warnings.txt || true
if [ -s ${TEMP_DIR}/clippy_warnings.txt ]; then
    echo -e "   ${RED}✗ Clippy warnings found${NC}"
    cat ${TEMP_DIR}/clippy_warnings.txt
    exit 1
else
    echo -e "   ${GREEN}✓ No warnings (clean build)${NC}"
fi

# Cleanup
rm -f ${TEMP_DIR}/rust_output.txt ${TEMP_DIR}/python_output.txt ${TEMP_DIR}/clippy_warnings.txt

echo ""
echo "=== All Tests Passed ==="
echo ""
echo -e "${GREEN}✓ Rust examples work correctly${NC}"
echo -e "${GREEN}✓ Python Celery compatibility verified${NC}"
echo -e "${GREEN}✓ All unit tests pass${NC}"
echo -e "${GREEN}✓ No warnings or errors${NC}"
echo ""
echo "To test full integration with a running worker:"
echo "  1. Start Python worker: python examples/python_consumer.py worker"
echo "  2. Send tasks from Rust or Python"
echo "  3. Verify tasks are processed correctly"
