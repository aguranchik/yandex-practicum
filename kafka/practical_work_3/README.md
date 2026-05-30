# Практическая работа 3

Тема работы: потоковая обработка сообщений в Kafka с использованием Goka.

## Что делает приложение

Приложение реализует цепочку обработки:

```text
messages -> censorship processor -> censored_messages -> block filter processor -> filtered_messages
```

Дополнительно работают два процессора состояния:

- `BlockListProcessor` читает события из `blocked_users` и хранит отдельный список блокировок для каждого пользователя в Goka group table `pw3-block-list-table`.
- `BannedWordsProcessor` читает обновления из `banned_words` и хранит актуальный список запрещенных слов в Goka group table `pw3-banned-words-table`.

## Структура проекта

```text
practical_work_3/
├── Dockerfile
├── README.md
├── banned_words_processor.go
├── block_filter_processor.go
├── block_list_processor.go
├── codecs.go
├── config.go
├── docker-compose.app.yaml
├── docker-compose.yaml
├── go.mod
├── main.go
├── models.go
├── state.go
├── test-data.txt
└── topic.txt
```

## Форматы сообщений

Входящие сообщения в `messages`:

```json
{"message_id":"msg-001","sender_id":"alice","recipient_id":"bob","text":"hello spam","created_at":"2026-05-15T00:02:00Z"}
```

События блокировки в `blocked_users`:

```json
{"user_id":"bob","blocked_user_id":"alice","action":"block","updated_at":"2026-05-15T00:01:00Z"}
```

Для разблокировки используется `action: "unblock"`.

Обновление запрещенных слов в `banned_words`:

```json
{"words":["spam","scam","badword"],"action":"replace","updated_at":"2026-05-15T00:00:00Z"}
```

Также поддерживаются `action: "add"` и `action: "remove"`.

## Важное правило ключей

Сообщения должны отправляться с ключом получателя:

- для `messages` ключ равен `recipient_id`;
- для `blocked_users` ключ равен `user_id`;
- для `banned_words` ключ равен `global`.

Это нужно, чтобы Goka могла корректно использовать group table: список блокировок пользователя находится в той же партиции, что и сообщения этому пользователю.

## Как запустить Kafka-кластер

```bash
cd /Users/guranchik/yandex-practicum/kafka/practical_work_3
docker compose down -v
docker compose up -d
docker compose ps
```

AKHQ будет доступен по адресу:

```text
http://localhost:8080
```

## Как создать топики

Выполнить команды из `topic.txt`.

Основные топики:

- `messages` - входящие сообщения;
- `filtered_messages` - сообщения после цензуры и фильтрации блокировок;
- `blocked_users` - события блокировки пользователей;
- `banned_words` - динамические обновления запрещенных слов;
- `censored_messages` - внутренний топик между цензурой и фильтром блокировок.

State store топики Goka:

- `pw3-block-list-table`;
- `pw3-banned-words-table`.

## Как запустить приложение

```bash
docker compose -f docker-compose.app.yaml up -d --build
docker compose -f docker-compose.app.yaml logs -f app
```

Остановить приложение:

```bash
docker compose -f docker-compose.app.yaml down
```

## Как отправить тестовые данные

Отправить список запрещенных слов:

```bash
printf 'global\t{"words":["spam","scam","badword"],"action":"replace","updated_at":"2026-05-15T00:00:00Z"}\n' | \
docker compose exec -T kafka-1 kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic banned_words \
  --property parse.key=true \
  --property key.separator=$'\t'
```

Заблокировать Alice для Bob:

```bash
printf 'bob\t{"user_id":"bob","blocked_user_id":"alice","action":"block","updated_at":"2026-05-15T00:01:00Z"}\n' | \
docker compose exec -T kafka-1 kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic blocked_users \
  --property parse.key=true \
  --property key.separator=$'\t'
```

Отправить сообщения:

```bash
printf 'bob\t{"message_id":"msg-001","sender_id":"alice","recipient_id":"bob","text":"hello Bob, this spam should not reach you","created_at":"2026-05-15T00:02:00Z"}\n' | \
docker compose exec -T kafka-1 kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic messages \
  --property parse.key=true \
  --property key.separator=$'\t'

printf 'bob\t{"message_id":"msg-002","sender_id":"carol","recipient_id":"bob","text":"this scam and badword must be hidden","created_at":"2026-05-15T00:03:00Z"}\n' | \
docker compose exec -T kafka-1 kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic messages \
  --property parse.key=true \
  --property key.separator=$'\t'

printf 'carol\t{"message_id":"msg-003","sender_id":"alice","recipient_id":"carol","text":"clean message for Carol","created_at":"2026-05-15T00:04:00Z"}\n' | \
docker compose exec -T kafka-1 kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic messages \
  --property parse.key=true \
  --property key.separator=$'\t'
```

Ожидаемый результат:

- `msg-001` не попадет в `filtered_messages`, потому что Bob заблокировал Alice;
- `msg-002` попадет в `filtered_messages`, но слова `scam` и `badword` будут замаскированы;
- `msg-003` попадет в `filtered_messages` без изменений.

Проверить итоговые сообщения:

```bash
docker compose exec -T kafka-1 kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic filtered_messages \
  --from-beginning \
  --property print.key=true \
  --property key.separator=' -> '
```

Дополнительные тестовые строки собраны в `test-data.txt`.
