FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY transform_service/requirements.txt /app/transform_service/requirements.txt
RUN pip install --no-cache-dir -r /app/transform_service/requirements.txt

COPY transform_service /app/transform_service

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "transform_service.app.main"]
