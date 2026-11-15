FROM python:3.12-slim-bookworm

WORKDIR /app

COPY query_service/requirements.txt /app/query_service/requirements.txt
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /app/query_service/requirements.txt

COPY query_service /app/query_service
COPY eventpipe /app/eventpipe

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "-m", "query_service.app.main"]
