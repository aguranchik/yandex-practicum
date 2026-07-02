# Скриншоты для ревью

Скриншоты сделаны во время работающего облачного стенда:

1. `01-schema-subjects.png` — список subjects Schema Registry:

   ```bash
   curl --user "$SCHEMA_REGISTRY_USERNAME:$SCHEMA_REGISTRY_PASSWORD" \
     http://localhost:8081/subjects
   ```

2. `02-schema-versions.png` — версии схемы:

   ```bash
   curl --user "$SCHEMA_REGISTRY_USERNAME:$SCHEMA_REGISTRY_PASSWORD" \
     http://localhost:8081/subjects/pw7-events-value/versions
   ```

3. `03-producer.png` — пять сообщений, успешно отправленных Go-продюсером.
4. `04-consumer.png` — пять сообщений, прочитанных и декодированных
   Go-консьюмером.
5. `05-nifi-flow.png` — NiFi canvas с потоком
   `Generate PW7 Events -> Publish to Managed Kafka`; оба процессора работают.
6. `06-nifi-kafka-consumer.png` — файл
   `nifi-kafka-console-consumer.log` с тремя сообщениями.
7. `07-cloud-kafka.png` — три Kafka-брокера со статусом `ALIVE` в трёх зонах.

Для NiFi и Schema Registry используется один SSH-туннель:

```bash
source .runtime.env
ssh \
  -L 8087:127.0.0.1:8080 \
  -L 8081:127.0.0.1:8081 \
  "yc-user@$VM_PUBLIC_IP"
```

NiFi UI: `http://localhost:8087/nifi/`.

После получения скриншотов облачный стенд удалён.
