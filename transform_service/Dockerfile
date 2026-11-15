FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY transform_service/requirements.txt /app/transform_service/requirements.txt
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /app/transform_service/requirements.txt

COPY transform_service /app/transform_service
COPY alembic.ini /app/alembic.ini
COPY migrations /app/migrations
COPY scripts/adopt_legacy_schema.py /app/scripts/adopt_legacy_schema.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "-m", "transform_service.app.main"]
