#!/bin/bash
# Validate USD assets in the usd-wg-assets submodule.
#
# This script iterates through each asset directory and passes it to
# the Rust validator, which handles finding root files and composition.
#
# Usage:
#   ./scripts/validate-usd-wg-assets.sh [OPTIONS]
#
# Options:
#   -v, --verbose     Show detailed output for each file
#   -a, --assets      Check that referenced assets exist
#   -f, --fail-fast   Stop on first error

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPENUSD_DIR="$(dirname "$SCRIPT_DIR")"
ASSETS_DIR="$OPENUSD_DIR/vendor/usd-wg-assets"

# Assets to skip (corrupted or known issues)
SKIP_ASSETS="CarbonFrameBike"

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

# Build the validator first
echo "Building validator..."
cd "$OPENUSD_DIR"
cargo build --release --features cli --bin validate_usd 2>&1 | grep -v "^warning:" || true
VALIDATOR="$OPENUSD_DIR/target/release/validate_usd"

echo ""
echo "USD Working Group Assets Validation"
echo "===================================="
echo "Assets directory: $ASSETS_DIR"
echo ""

# Parse arguments - pass through to validator
VALIDATOR_ARGS=""
for arg in "$@"; do
    VALIDATOR_ARGS="$VALIDATOR_ARGS $arg"
done

# Check if an asset should be skipped
should_skip() {
    local asset_name="$1"
    echo "$SKIP_ASSETS" | grep -qw "$asset_name"
}

# Track statistics
total_assets=0
passed_assets=0
failed_assets=0

# Validate full_assets
echo "Full Assets"
echo "-----------"
if [ -d "$ASSETS_DIR/full_assets" ]; then
    for asset_dir in "$ASSETS_DIR/full_assets"/*/; do
        if [ -d "$asset_dir" ]; then
            asset_name="$(basename "$asset_dir")"

            if should_skip "$asset_name"; then
                echo "  [SKIP] $asset_name (in skip list)"
                continue
            fi

            total_assets=$((total_assets + 1))

            # Run validator in asset mode
            if $VALIDATOR "$asset_dir" --asset $VALIDATOR_ARGS 2>&1 | grep -q "Failed:.*[1-9]\|Passed:.*0"; then
                echo "  [FAIL] $asset_name"
                failed_assets=$((failed_assets + 1))
            else
                echo "  [PASS] $asset_name"
                passed_assets=$((passed_assets + 1))
            fi
        fi
    done
fi
echo ""

# Validate test_assets
echo "Test Assets"
echo "-----------"
if [ -d "$ASSETS_DIR/test_assets" ]; then
    for asset_dir in "$ASSETS_DIR/test_assets"/*/; do
        if [ -d "$asset_dir" ]; then
            asset_name="$(basename "$asset_dir")"

            # Skip internal directories like _common
            case "$asset_name" in
                _*) continue ;;
            esac

            if should_skip "$asset_name"; then
                echo "  [SKIP] $asset_name (in skip list)"
                continue
            fi

            total_assets=$((total_assets + 1))

            # Run validator in asset mode
            if $VALIDATOR "$asset_dir" --asset $VALIDATOR_ARGS 2>&1 | grep -q "Failed:.*[1-9]\|Passed:.*0"; then
                echo "  [FAIL] $asset_name"
                failed_assets=$((failed_assets + 1))
            else
                echo "  [PASS] $asset_name"
                passed_assets=$((passed_assets + 1))
            fi
        fi
    done
fi
echo ""

# Print summary
echo "===================================="
echo "Summary"
echo "===================================="
echo "Total assets:  $total_assets"
echo "Passed:        $passed_assets"
echo "Failed:        $failed_assets"
echo ""

if [ $failed_assets -gt 0 ]; then
    exit 1
fi
