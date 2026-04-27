FROM python:3.11-slim

WORKDIR /app

# Install package
COPY src/python/ /app/
RUN pip install --no-cache-dir -e .

# Create non-root user for mounted results directory writes
RUN useradd -m -u 1000 benchmark && \
    mkdir -p /app/results && \
    chown -R benchmark:benchmark /app/results

USER benchmark

CMD ["python", "-m", "benchmark.generator.producer"]
