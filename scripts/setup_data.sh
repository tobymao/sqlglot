#!/usr/bin/env bash
# Download and set up Spider 1.0 dataset for validation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data/spider"

SPIDER_GDRIVE_ID="1TqleXec_OykOYFREKKtschzY29dUcVAQ"

echo "=== Spider 1.0 Dataset Setup ==="

# Check for gdown
if ! command -v gdown &>/dev/null; then
    echo "Error: gdown is not installed. Install it with: pip install gdown"
    exit 1
fi

if [ -d "$DATA_DIR" ] && [ -f "$DATA_DIR/dev.json" ]; then
    echo "Spider dataset already exists at $DATA_DIR"
    echo "To re-download, remove the directory first: rm -rf $DATA_DIR"
    exit 0
fi

mkdir -p "$DATA_DIR"

TMPFILE="$(mktemp /tmp/spider_XXXXXX.zip)"
trap 'rm -f "$TMPFILE"' EXIT

echo "Downloading Spider 1.0 from Google Drive..."
gdown "$SPIDER_GDRIVE_ID" -O "$TMPFILE"

echo "Extracting to $DATA_DIR..."
unzip -q "$TMPFILE" -d "$DATA_DIR"

# Spider zip extracts into a spider/ subdirectory — flatten it
if [ -d "$DATA_DIR/spider" ]; then
    mv "$DATA_DIR/spider"/* "$DATA_DIR/"
    rm -rf "$DATA_DIR/spider"
fi

# Verify expected structure
MISSING=()
for f in dev.json train_spider.json tables.json; do
    [ -f "$DATA_DIR/$f" ] || MISSING+=("$f")
done
[ -d "$DATA_DIR/database" ] || MISSING+=("database/")

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "Warning: missing expected files: ${MISSING[*]}"
    echo "The archive structure may have changed. Listing contents:"
    ls -la "$DATA_DIR"
    exit 1
fi

DB_COUNT=$(find "$DATA_DIR/database" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
echo ""
echo "=== Setup Complete ==="
echo "  Location:    $DATA_DIR"
echo "  dev.json:    $(wc -l < "$DATA_DIR/dev.json") lines"
echo "  Databases:   $DB_COUNT"
echo ""
echo "Run validation with:"
echo "  python -m validation.run --data $DATA_DIR/dev.json --db-dir $DATA_DIR/database --source spider --baseline"
