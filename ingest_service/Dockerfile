FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ingest_service/requirements.txt /app/ingest_service/requirements.txt
RUN pip install --no-cache-dir -r /app/ingest_service/requirements.txt

COPY proto /app/proto
COPY ingest_service /app/ingest_service
COPY ingest_service/pytest.ini /app/pytest.ini

RUN PROTO_INC=$(python -c "from pathlib import Path; import google.protobuf; print(Path(google.protobuf.__file__).resolve().parent.parent.parent)") && \
    mkdir -p /app/ingest_service/app/generated && \
    python -m grpc_tools.protoc \
      -I /app/proto \
      -I "$PROTO_INC" \
      --python_out=/app/ingest_service/app/generated \
      --grpc_python_out=/app/ingest_service/app/generated \
      /app/proto/event.proto && \
    python -c "import pathlib; p=pathlib.Path('/app/ingest_service/app/generated/event_pb2_grpc.py'); p.write_text(p.read_text().replace('import event_pb2 as event__pb2', 'from . import event_pb2 as event__pb2', 1))"

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000 50051

CMD ["python", "-m", "ingest_service.app.main"]
