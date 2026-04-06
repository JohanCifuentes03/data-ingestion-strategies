from setuptools import setup, find_packages

setup(
    name="benchmark",
    version="1.0.0",
    description="Data ingestion strategies benchmark tools",
    author="Data Ingestion Research Team",
    packages=find_packages(),
    install_requires=[
        "confluent-kafka>=2.3.0",
        "matplotlib>=3.8",
        "numpy>=1.26",
        "pandas>=2.1",
        "prometheus-client>=0.19.0",
        "psycopg2-binary>=2.9.9",
        "pyyaml>=6.0.1",
        "scipy>=1.12",
        "seaborn>=0.13",
    ],
    entry_points={
        "console_scripts": [
            "benchmark-probe=benchmark.probe.availability_probe:main",
            "benchmark-analyze=benchmark.analysis.analyzer:main",
            "benchmark-validate=benchmark.validation.validator:main",
            "benchmark-generate=benchmark.generator.producer:main",
        ],
    },
    python_requires=">=3.11",
)
