# Практическая работа 7

Решение разворачивает минимальный production-like стенд в Yandex Cloud:

- три брокера Managed Service for Apache Kafka;
- KRaft в combined mode, без отдельных controller VM;
- встроенный Managed Schema Registry;
- Avro-продюсер и консьюмер на Go;
- Apache NiFi на одной VM;
- приватный доступ к Kafka и SSH-туннели к UI/API.

## Ресурсы

| Компонент | Конфигурация | Количество |
|---|---|---:|
| Kafka broker | `b2.medium`: 2 vCPU (50%), 4 ГБ RAM | 3 |
| Kafka disk | `network-hdd`, 10 ГБ | 3 |
| NiFi VM | 2 vCPU (20%), 4 ГБ RAM, preemptible | 1 |
| NiFi boot disk | `network-hdd`, 20 ГБ | 1 |

Брокеры размещаются по одному в `ru-central1-a`, `ru-central1-b` и
`ru-central1-d`. Kafka не получает публичных IP-адресов. VM находится в той же
VPC и имеет единственный публичный адрес только для SSH.

## Параметры Kafka

Параметры кластера:

```text
compression.type=zstd
log.retention.ms=86400000
log.segment.bytes=134217728
num.partitions=3
default.replication.factor=3
auto.create.topics.enable=false
```

Топики `pw7-events` и `pw7-nifi`:

```text
partitions=3
replication.factor=3
cleanup.policy=delete
retention.ms=86400000
retention.bytes=268435456
segment.bytes=134217728
min.insync.replicas=2
compression.type=zstd
```

Ограничение `retention.bytes` не даёт лабораторным данным заполнить минимальные
диски. Продюсер использует `acks=all` и идемпотентную отправку.

## Структура

```text
cmd/producer/             Avro-продюсер
cmd/consumer/             Avro-консьюмер
internal/                 Kafka, TLS, Avro и Schema Registry helpers
schemas/pw7-event.avsc    Avro-схема
config/cloud-init/        подготовка VM
config/kafka/             Kafka CLI client properties
config/nifi/              Docker Compose и инструкция NiFi
scripts/                  создание, проверка и удаление стенда
artifacts/                логи и скриншоты для ревью
```

Секреты хранятся только в `.runtime.env` и `/opt/pw7/.env`; оба файла исключены
из Git.

## Состав решения

| Требование | Файлы и подтверждения |
|---|---|
| 3 брокера и аппаратные ресурсы | `scripts/create-cloud.sh`, `artifacts/logs/kafka-cluster.yaml`, `artifacts/logs/kafka-hosts.yaml` |
| Топик: 3 партиции, RF=3, очистка и хранение | `scripts/configure-cloud.sh`, `artifacts/logs/topic-pw7-events.yaml`, `artifacts/logs/kafka-topics-describe.log` |
| Schema Registry и Avro-схема | `schemas/pw7-event.avsc`, `artifacts/logs/schema-registry-*.json`, `artifacts/screenshots/01-*.png`, `artifacts/screenshots/02-*.png` |
| Продюсер и консьюмер | `cmd/producer`, `cmd/consumer`, `artifacts/logs/producer.log`, `artifacts/logs/consumer.log`, скриншоты `03` и `04` |
| Интеграция NiFi | `config/nifi/docker-compose.yaml`, `config/nifi/README.md`, `artifacts/logs/nifi-flow-definition.json`, `artifacts/logs/nifi-kafka-console-consumer.log`, скриншоты `05` и `06` |
| Работающий облачный кластер | `artifacts/screenshots/07-cloud-kafka.png`, `artifacts/logs/cloud-resources-running.log` |

## Локальная проверка

```bash
go test ./...
./scripts/build-linux.sh
docker compose -f config/nifi/docker-compose.yaml config
```

## Подготовка облака

```bash
cp .env.example .env
# Указать ADMIN_CIDR.

./scripts/preflight.sh
CONFIRM_PAID_RESOURCES=YES ./scripts/create-cloud.sh
./scripts/configure-cloud.sh
./scripts/deploy-to-vm.sh
```

## Проверка задания 1

```bash
./scripts/run-e2e.sh
./scripts/describe-topic.sh
./scripts/capture-schema-registry.sh
./scripts/schema-registry-tunnel.sh
```

Последняя команда подготавливает туннель, через который для скриншотов можно
вызвать `http://localhost:8081/subjects`.

## Проверка задания 2

```bash
./scripts/start-nifi.sh
```

Настройка flow описана в `config/nifi/README.md`. После запуска потока сообщения
из NiFi поступают в `pw7-nifi` и читаются Kafka CLI/Go-консьюмером.

## Аварийная остановка и удаление

Остановить вычисления, оставив диски:

```bash
./scripts/stop-paid-resources.sh
```

Полностью удалить стенд:

```bash
CONFIRM_DESTROY=DELETE ./scripts/destroy-cloud.sh
```

## Результаты проверки

Проверка.

- Managed Kafka cluster ID: `c9q8igmn84u5og8psg3b`.
- Статус кластера: `RUNNING`, health: `ALIVE`.
- Версия Kafka: `3.9.2`.
- Три брокера находятся в `ru-central1-a`, `ru-central1-b`,
  `ru-central1-d`.
- Схема `pw7-events-value` зарегистрирована с ID `1`.
- Go-продюсер отправил пять Avro-сообщений с `acks=all`.
- Go-консьюмер прочитал и декодировал пять сообщений из всех трёх партиций.
- Все реплики топика `pw7-events` находятся в ISR.
- NiFi flow `Generate PW7 Events -> Publish to Managed Kafka` передал
  JSON-сообщения в `pw7-nifi` по SASL_SSL/SCRAM-SHA-512.
- `kafka-console-consumer.sh` прочитал три сообщения, отправленные NiFi.

Подтверждающие файлы находятся в `artifacts/logs/`:

- `kafka-cluster.yaml`, `kafka-hosts.yaml` — конфигурация и хосты;
- `topic-pw7-events.yaml`, `kafka-topics-describe.log` — параметры топика и ISR;
- `producer.log`, `consumer.log` — успешная передача Avro-сообщений;
- `schema-registry-subjects.json`, `schema-registry-versions.json` — ответы API;
- `nifi-flow-definition.json` — экспорт flow без чувствительных свойств;
- `nifi-flow-status.json` — статус NiFi с `flowFilesSent=3`;
- `nifi-kafka-console-consumer.log` — сообщения NiFi в Kafka;
- `cloud-resources-deleted.log` — контрольная проверка после удаления стенда.

Скриншоты ответов Schema Registry, логов приложений, работающего NiFi flow и
трёх Kafka-брокеров находятся в `artifacts/screenshots/`.

После проверки стенд удалён.
