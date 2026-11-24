FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    ISTATSYNC_OUTPUT_DIR=/tmp/istatsync

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[drive]"

COPY datasets.toml ./

RUN useradd --create-home runner
USER runner

# Cloud Run Job: one execution = one full pass over the catalogue.
# Exit code is non-zero if any dataset failed, so the execution shows as failed.
ENTRYPOINT ["istatsync"]
CMD ["run"]
