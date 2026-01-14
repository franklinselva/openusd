#!/bin/bash
# Validate all USD files in the usd-wg-assets submodule.
#
# Usage:
#   ./scripts/validate-usd-wg-assets.sh [OPTIONS]
#
# Options are passed through to the validate_usd binary:
#   -v, --verbose     Show detailed output for each file
#   -s, --summary     Only show summary statistics
#   -c, --composed    Use composition layer (resolves sublayers)
#   -f, --fail-fast   Stop on first error
#   -x, --skip <PAT>  Skip files matching pattern

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPENUSD_DIR="$(dirname "$SCRIPT_DIR")"
ASSETS_DIR="$OPENUSD_DIR/vendor/usd-wg-assets"

# Ensure submodule is initialized
if [ ! -d "$ASSETS_DIR/.git" ] && [ ! -f "$ASSETS_DIR/.git" ]; then
    echo "Initializing usd-wg-assets submodule..."
    cd "$OPENUSD_DIR"
    git submodule update --init --recursive vendor/usd-wg-assets
fi

# Check if assets directory has content
if [ ! -d "$ASSETS_DIR/test_assets" ]; then
    echo "Error: usd-wg-assets submodule appears empty."
    echo "Try running: cd $OPENUSD_DIR && git submodule update --init --recursive"
    exit 1
fi

echo "USD Working Group Assets Validation"
echo "===================================="
echo "Assets directory: $ASSETS_DIR"
echo ""

# Build and run the validator with cli feature
# Default options: -a (check assets), -c (composed/full composition)
cd "$OPENUSD_DIR"
cargo run --release --features cli --bin validate_usd -- "$ASSETS_DIR" -a -c "$@"
