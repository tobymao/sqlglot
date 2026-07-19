FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml setup.cfg setup.py MANIFEST.in README.md ./
COPY sqlglot ./sqlglot
COPY sqlglotc ./sqlglotc
COPY tests ./tests

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -e . pytest

CMD ["python", "-m", "pytest", "tests/test_tokens.py", "-q"]
