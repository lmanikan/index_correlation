.PHONY: help install test lint format run clean

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linters"
	@echo "  make format     - Format code"
	@echo "  make run        - Run pipeline"
	@echo "  make clean      - Clean artifacts"

install:
	pip install -e .

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

format:
	black src/ tests/
	isort src/ tests/

run:
	python examples/basic_run.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache htmlcov .coverage
	rm -rf build/ dist/ *.egg-info/
