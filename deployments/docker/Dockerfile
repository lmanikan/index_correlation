FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv pip install -e .

COPY . .

CMD ["python", "examples/basic_run.py"]
