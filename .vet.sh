#!/bin/bash
set -e

echo "🚀 Starting verification (vetting) for $(basename "$PWD")..."

# Check if ruff is installed
if ! command -v ruff &> /dev/null; then
    echo "❌ Error: 'ruff' is not installed. Please install it with 'pip install ruff' or 'uv pip install ruff'."
    exit 1
fi

echo "🔍 Running Ruff Linting (Check)..."
ruff check .

echo "🎨 Running Ruff Formatting (Check)..."
ruff format --check .

echo "✅ All checks passed!"
