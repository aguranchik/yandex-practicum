# Практическая работа 5

Тема работы: настройка Debezium PostgreSQL Connector для передачи изменений из PostgreSQL в Kafka через CDC и сбор метрик Kafka Connect.

## Что делает проект

Проект поднимает локальную инфраструктуру:

- Kafka-кластер из трех брокеров в KRaft-режиме.
- PostgreSQL с включенным `wal_level = logical`.
- Kafka Connect на базе Debezium Connect с PostgreSQL Connector.
- Prometheus для сбора JMX-метрик Kafka Connect.
- Grafana с готовым dashboard для Kafka Connect CDC.
- Go-консьюмер, который читает CDC-события из Kafka-топиков и выводит их в терминал.

Поток данных:

```text
PostgreSQL users/orders -> Debezium PostgreSQL Connector -> Kafka topics -> Go CDC consumer
                                                     |
                                                     v
                                           JMX Exporter -> Prometheus -> Grafana
```

## Структура проекта

```text
practical_work_5/
├── README.md
├── connector.json
├── docker-compose.app.yaml
├── docker-compose.yaml
├── test-data.sql
├── consumer/
│   ├── Dockerfile
│   ├── config.go
│   ├── config_test.go
│   ├── consumer.go
│   ├── go.mod
│   └── main.go
├── grafana/
│   ├── dashboards/
│   │   └── kafka-connect-cdc.json
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboards.yml
│       └── datasources/
│           └── prometheus.yml
├── kafka-connect/
│   ├── Dockerfile
│   ├── jmx_prometheus_javaagent-0.15.0.jar
│   └── kafka-connect.yml
├── postgres/
│   ├── custom-config.conf
│   └── init.sql
└── prometheus/
    └── prometheus.yml
```

## Компоненты

### Kafka

Kafka принимает CDC-события от Debezium. Внутри compose сервисы доступны как:

- `kafka-0:9092`
- `kafka-1:9092`
- `kafka-2:9092`

Снаружи брокеры доступны на портах `19094`, `19095`, `19096`.

### PostgreSQL

PostgreSQL использует базу `shop`. При первом старте выполняется `postgres/init.sql`, где создаются таблицы:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

В `postgres/custom-config.conf` включен logical WAL:

```conf
wal_level = 'logical'
max_wal_senders = 4
max_replication_slots = 4
```

Это нужно Debezium, чтобы читать изменения из журнала транзакций PostgreSQL.

### Kafka Connect и Debezium

Kafka Connect запускается из образа `quay.io/debezium/connect:2.5`. Образ уже содержит Debezium PostgreSQL Connector.

Файл `connector.json` настраивает коннектор `postgres-cdc-source`:

- `database.hostname`: `postgres`
- `database.dbname`: `shop`
- `plugin.name`: `pgoutput`
- `slot.name`: `pw5_debezium_slot`
- `publication.name`: `pw5_publication`
- `table.include.list`: `public.users,public.orders`
- `snapshot.mode`: `initial`
- `topic.prefix`: `postgres`

Коннектор отслеживает только таблицы:

- `public.users`
- `public.orders`

Debezium создает Kafka-топики:

- `postgres.public.users`
- `postgres.public.orders`

### Prometheus и Grafana

В образ Kafka Connect добавлен JMX Exporter. Он публикует метрики на `kafka-connect:9876/metrics`.

Prometheus собирает эти метрики по job `kafka-connect`.

Grafana доступна на `http://localhost:13000`. Dashboard называется `Practical Work 5 - Kafka Connect CDC`.

На dashboard есть графики:

- статус коннектора;
- скорость чтения/записи source records;
- среднее время batch poll.

## Как запустить инфраструктуру

Перейти в каталог проекта:

```bash
cd /Users/guranchik/yandex-practicum/kafka/practical_work_5
```

Если нужно начать с чистого состояния:

```bash
docker compose down -v
```

Поднять инфраструктуру:

```bash
docker compose up -d --build
```

Проверить контейнеры:

```bash
docker compose ps
```

Интерфейсы:

- AKHQ: `http://localhost:18080`
- Kafka Connect REST API: `http://localhost:18083`
- Prometheus: `http://localhost:19090`
- Grafana: `http://localhost:13000`
- PostgreSQL: `localhost:15432`

## Как проверить PostgreSQL

Подключиться к базе:

```bash
docker compose exec -T postgres psql -U postgres-user -d shop
```

Проверить таблицы:

```sql
\dt
SELECT * FROM users;
SELECT * FROM orders;
```

## Как настроить Debezium Connector

Проверить доступность Kafka Connect:

```bash
curl -s http://localhost:18083/connectors
```

Проверить, что PostgreSQL Connector установлен:

```bash
curl -s http://localhost:18083/connector-plugins/
```

Создать коннектор:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @connector.json \
  http://localhost:18083/connectors
```

Если коннектор уже существует, его можно пересоздать:

```bash
curl -X DELETE http://localhost:18083/connectors/postgres-cdc-source

curl -X POST \
  -H "Content-Type: application/json" \
  --data @connector.json \
  http://localhost:18083/connectors
```

Проверить статус:

```bash
curl -s http://localhost:18083/connectors/postgres-cdc-source/status
```

Ожидаемый результат: `connector.state` и `tasks[0].state` равны `RUNNING`.

## Как посмотреть Kafka-топики

```bash
docker compose exec -T kafka-1 \
  kafka-topics.sh --bootstrap-server kafka-1:9092 --list
```

Ожидаемые CDC-топики:

```text
postgres.public.users
postgres.public.orders
```

## Как запустить Go-консьюмер

Консьюмер читает оба CDC-топика и печатает события в терминал.

```bash
docker compose -f docker-compose.app.yaml up -d --build
docker compose -f docker-compose.app.yaml logs -f cdc-consumer
```

Остановить консьюмер:

```bash
docker compose -f docker-compose.app.yaml down
```

## Как добавить тестовые данные

Вставить дополнительные записи и изменения:

```bash
docker compose exec -T postgres psql -U postgres-user -d shop < test-data.sql
```

Или выполнить вручную:

```sql
INSERT INTO users (name, email) VALUES ('Charlie Green', 'charlie@example.com');
INSERT INTO users (name, email) VALUES ('Diana White', 'diana@example.com');

INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product F', 7);
INSERT INTO orders (user_id, product_name, quantity) VALUES (5, 'Product G', 2);

UPDATE users SET email = 'john.doe@example.com' WHERE id = 1;
UPDATE orders SET quantity = 10 WHERE id = 1;

DELETE FROM orders WHERE id = 2;
```

После выполнения SQL в логах `cdc-consumer` должны появиться новые события из топиков `postgres.public.users` и `postgres.public.orders`.

## Как проверить метрики

Проверить endpoint JMX Exporter:

```bash
curl -s http://localhost:19876/metrics | grep kafka_connect_source_task_source_record_write_total
```

Проверить Prometheus target:

```text
http://localhost:19090/targets
```

В Grafana открыть:

```text
http://localhost:13000/d/pw5-kafka-connect-cdc/practical-work-5-kafka-connect-cdc
```

Логин и пароль, если понадобятся:

```text
admin / admin
```

## Локальная проверка Go-кода

```bash
cd /Users/guranchik/yandex-practicum/kafka/practical_work_5/consumer
GOCACHE=/private/tmp/go-build-practical-work-5 go test ./...
```
