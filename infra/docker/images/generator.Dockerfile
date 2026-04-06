FROM python:3.11-slim

WORKDIR /app

# Install package
COPY src/python/ /app/
RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "benchmark.generator.producer"]
