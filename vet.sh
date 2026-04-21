#!/bin/bash
set -e

echo "🚀 Starting verification (vetting) for $(basename "$PWD")..."

echo "🔍 Running Ruff Linting (Check)..."
uv run ruff check .

echo "🎨 Running Ruff Formatting (Check)..."
uv run ruff format --check .

echo "🧪 Running Tests (Pytest)..."
uv run pytest

echo "✅ All checks passed!"
