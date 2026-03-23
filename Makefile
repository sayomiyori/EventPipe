.PHONY: proto up down logs test

PROTO_ROOT := proto
GEN_OUT := ingest_service/app/generated

proto:
	@python -c "import pathlib; pathlib.Path('$(GEN_OUT)').mkdir(parents=True, exist_ok=True)"
	@python -m grpc_tools.protoc \
		-I $(PROTO_ROOT) \
		-I $$(python -c "from pathlib import Path; import google.protobuf; print(Path(google.protobuf.__file__).resolve().parent.parent.parent)") \
		--python_out=$(GEN_OUT) \
		--grpc_python_out=$(GEN_OUT) \
		$(PROTO_ROOT)/event.proto
	@python -c "import pathlib; p=pathlib.Path('$(GEN_OUT)/event_pb2_grpc.py'); t=p.read_text(encoding='utf-8'); p.write_text(t.replace('import event_pb2 as event__pb2', 'from . import event_pb2 as event__pb2', 1), encoding='utf-8')"

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f ingest_service

test:
	PYTHONPATH=$(CURDIR) python -m pytest ingest_service/tests -v
