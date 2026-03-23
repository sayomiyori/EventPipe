# EventPipe

Микросервисный ETL-пайплайн. На текущем этапе реализован **Ingest Service**: приём событий по **REST** и **gRPC**, публикация в **Apache Kafka**.

## Возможности

- **REST** (`FastAPI`): `POST /api/v1/events`, `POST /api/v1/events/batch`
- **gRPC**: `EventService.Ingest`, `EventService.IngestStream` (порт по умолчанию `50051`)
- **Kafka**: топик `events.raw`, ключ сообщения — `event_type`, значение — JSON
- **Protobuf**: схема в [`proto/event.proto`](proto/event.proto)

## Быстрый старт (Docker)

Из корня репозитория:

```bash
docker compose up -d --build
```

Сервис слушает **HTTP** на порту `8000` (или задайте маппинг в `docker-compose.yml`, если порт занят) и **gRPC** на `50051`. Kafka доступна с хоста на `localhost:29092`.

Переменные окружения (префикс `EVENTPIPE_`): см. [`ingest_service/app/config.py`](ingest_service/app/config.py).

## Локальная разработка

```bash
pip install -r ingest_service/requirements.txt
make proto   # генерация Python из .proto (нужен grpcio-tools)
PYTHONPATH=. python -m ingest_service.app.main
```

Тесты:

```bash
make test
# или
PYTHONPATH=. python -m pytest ingest_service/tests -v
```

Интеграционный тест с Kafka: `pytest -m integration` (нужен доступный брокер, например из `docker compose`).

## Полезные команды

| Команда | Описание |
|--------|----------|
| `make proto` | Сгенерировать `ingest_service/app/generated/*` из `proto/event.proto` |
| `make up` / `make down` | Поднять / остановить стек в Docker |
| `make logs` | Логи сервиса `ingest_service` |

## Структура

```
eventpipe/
├── proto/                 # Protobuf
├── ingest_service/        # Ingest Service (FastAPI, gRPC, Kafka)
├── docker-compose.yml
└── Makefile
```

Репозиторий: [github.com/sayomiyori/EventPipe](https://github.com/sayomiyori/EventPipe).
