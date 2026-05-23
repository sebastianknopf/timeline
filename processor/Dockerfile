FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# setuptools_scm needs git metadata available at build/runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && git config --global core.autocrlf true \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/
COPY src /app/src
COPY alembic.ini /app/
COPY alembic /app/alembic

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e .

CMD ["python", "-m", "processor"]