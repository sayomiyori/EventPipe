FROM python:3.12-slim-bookworm

WORKDIR /app

COPY query_service/requirements.txt /app/query_service/requirements.txt
RUN pip install --no-cache-dir -r /app/query_service/requirements.txt

COPY query_service /app/query_service

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "query_service.app.main"]
