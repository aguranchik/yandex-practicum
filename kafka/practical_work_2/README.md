# Практическая работа 2

Тема работы: настройка кластера Kafka и реализация приложения с одним продюсером и двумя консьюмерами.

## Структура проекта

```text
practical_work_2/
├── Dockerfile
├── README.md
├── config.go
├── consumer_batch.go
├── consumer_single.go
├── docker-compose.app.yaml
├── docker-compose.yaml
├── go.mod
├── go.sum
├── main.go
├── message.go
├── producer.go
├── topic.txt
└── utils.go
```

## Что делает приложение

Один экземпляр приложения содержит:

- `Producer`
- `SingleMessageConsumer`
- `BatchMessageConsumer`

Приложение запускается в двух экземплярах через `docker compose ... --scale app=2`.

Итоговая система после масштабирования:

- 2 продюсера
- 2 `SingleMessageConsumer`
- 2 `BatchMessageConsumer`

## Логика работы компонентов

### Producer

Продюсер раз в секунду создает тестовое сообщение, сериализует его в JSON и асинхронно отправляет его в Kafka через `Produce()`.

Выбранные параметры:

- `acks=all`:
  продюсер ждет подтверждение записи от всех `in-sync replicas`, поэтому запись соответствует гарантии `At Least Once`.
- `retries=10`:
  клиент повторяет отправку при временных сбоях.

Продюсер выводит в лог:

- отправляемое сообщение
- результат доставки сообщения
- ошибки сериализации и доставки

### SingleMessageConsumer

- читает сообщения по одному
- десериализует JSON в структуру `Message`
- выводит сообщение в лог
- использует `enable.auto.commit=true`

Отдельный `group.id` позволяет ему читать те же сообщения, что и batch-консьюмер.

### BatchMessageConsumer

- получает сообщения через `Poll()`
- накапливает минимум 10 сообщений
- обрабатывает сообщения в цикле
- один раз делает `Commit()` после обработки пачки
- если в пачке возникла ошибка десериализации, пишет ее в лог и не выполняет `Commit()` для этой пачки

Дополнительные параметры:

- `fetch.min.bytes=1024`:
  брокер старается накопить не меньше 1024 байт перед ответом на fetch-запрос.
- `fetch.max.wait.ms=5000`:
  брокер ждет максимум 5 секунд перед ответом, если данных пока недостаточно.

В коде для Go-клиента `confluent-kafka-go` этот параметр передается как `fetch.wait.max.ms`, потому что библиотека использует имена конфигурации `librdkafka`.

Важно: в `confluent-kafka-go` `Consumer.Poll()` возвращает одно событие за вызов, а сообщения извлекаются по одному, хотя клиент заранее подкачивает их в фоне. Поэтому batch-логика реализована на уровне приложения: 10 сообщений накапливаются в буфере, после чего выполняется один общий `Commit()` для всей обработанной пачки.

## Формат сообщения

Вместо класса в Go используется структура:

```go
type Message struct {
    MessageID        string  `json:"message_id"`
    OrderID          string  `json:"order_id"`
    CustomerID       string  `json:"customer_id"`
    ProducerInstance string  `json:"producer_instance"`
    CreatedAt        string  `json:"created_at"`
    Status           string  `json:"status"`
    Amount           float64 `json:"amount"`
}
```

Продюсер сериализует структуру в JSON через `json.Marshal`, а оба консьюмера выполняют обратную операцию через `json.Unmarshal`.

Если сериализация или десериализация завершается ошибкой, приложение пишет сообщение в лог и продолжает работать.

## Как запустить Kafka-кластер

Перейти в каталог проекта:

```bash
cd /Users/guranchik/yandex-practicum/kafka/practical_work_2
```

Если ранее запускались старые контейнеры и volume:

```bash
docker compose down -v
```

Поднять Kafka-кластер:

```bash
docker compose up -d
```

Проверить состояние контейнеров:

```bash
docker compose ps
```

Интерфейс:

- AKHQ: `http://localhost:8080`

## Как создать топик

Топик создается вручную через консоль, как требует задание:

```bash
docker compose exec -T kafka-1 \
  kafka-topics.sh --create \
  --topic practical-work-2-orders \
  --bootstrap-server kafka-1:9092 \
  --partitions 3 \
  --replication-factor 2
```

Проверить описание топика:

```bash
docker compose exec -T kafka-1 \
  kafka-topics.sh --describe \
  --topic practical-work-2-orders \
  --bootstrap-server kafka-1:9092
```

Команды и итоговый вывод нужно сохранить в файл `topic.txt`.

## Как запустить приложение

Приложение запускается отдельным compose-файлом:

```bash
docker compose -f docker-compose.app.yaml up -d --build --scale app=2
```

Посмотреть логи:

```bash
docker compose -f docker-compose.app.yaml logs -f app
```

Остановить приложение:

```bash
docker compose -f docker-compose.app.yaml down
```

## Как проверить работу

1. Поднять Kafka-кластер.
2. Создать топик `practical-work-2-orders`.
3. Запустить приложение в двух экземплярах.
4. Проверить логи:
   - продюсер печатает отправляемые сообщения и delivery report
   - `SingleMessageConsumer` печатает каждое полученное сообщение
   - `BatchMessageConsumer` печатает сообщения пачкой и после этого пишет, что выполнил `Commit()`
5. Проверить consumer groups:

```bash
docker compose exec -T kafka-1 \
  kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --list
```

Ожидаются группы:

- `pw2-single-group`
- `pw2-batch-group`

Посмотреть offsets и lag:

```bash
docker compose exec -T kafka-1 \
  kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 \
  --group pw2-single-group --describe
```

```bash
docker compose exec -T kafka-1 \
  kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 \
  --group pw2-batch-group --describe
```

## Пояснение по `Poll()`

`Poll()` у консьюмера — это вызов, которым клиент Kafka запрашивает очередное событие.

В приложении он может вернуть:

- одно сообщение
- ошибку Kafka
- другое служебное событие
- `nil`, если по таймауту данных нет

Поэтому в batch-консьюмере пачка собирается не одним вызовом `Poll()`, а серией вызовов `Poll()` с накоплением 10 сообщений в памяти и последующим общим коммитом.
