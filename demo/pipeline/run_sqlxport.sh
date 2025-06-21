#!/bin/bash
set -eu

# ------------------------------
# Unified Pipeline Runner
# ------------------------------

# Cleanup
echo "🧼 Cleaning previous outputs..."
rm -rf pipeline_output/
rm -rf .mc-config/

# Run full pipeline
echo "🚀 Running full pipeline..."
./run_full_pipeline.sh "$@"
