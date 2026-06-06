# Практическая работа 6

Защищённый кластер Apache Kafka из трёх брокеров с шифрованием SSL/TLS,
аутентификацией SASL/PLAIN и разграничением доступа через ACL.

## Состав решения

- `zookeeper` хранит метаданные кластера и ACL.
- `kafka-1`, `kafka-2`, `kafka-3` образуют Kafka-кластер.
- `kafka-client` содержит Kafka CLI и клиентские настройки для проверки доступа.
- `akhq` предоставляет веб-интерфейс для просмотра кластера и топиков.
- `broker-config/` содержит отдельную конфигурацию каждого брокера.
- `client/` содержит SASL_SSL-настройки администратора, продюсера и консьюмера.
- `jaas-config/` содержит пользователей Kafka и учётную запись ZooKeeper.
- `certs/` содержит CA, сертификаты, ключи, Keystore и Truststore.
- `scripts/` содержит генерацию сертификатов, настройку топиков и тесты.

Внутри Docker-сети брокеры доступны по адресам `kafka-1:9092`,
`kafka-2:9092` и `kafka-3:9092`. С компьютера доступны SSL-порты:

| Брокер | Адрес |
|---|---|
| kafka-1 | `localhost:29092` |
| kafka-2 | `localhost:29093` |
| kafka-3 | `localhost:29094` |

## SSL и сертификаты

Для работы используется собственный центр сертификации `practical-work-6-ca`.
От него выпущены отдельные серверные сертификаты для трёх брокеров:

- `CN=kafka-1`;
- `CN=kafka-2`;
- `CN=kafka-3`.

Каждый брокер имеет:

- Keystore с закрытым ключом и серверным сертификатом;
- Truststore с сертификатом доверенного CA.

Клиенты используют общий Truststore `certs/client/kafka.truststore.jks`,
чтобы проверить сертификат брокера. SSL шифрует трафик, а пользователи
аутентифицируются отдельно через SASL/PLAIN. Пароль учебных хранилищ —
`changeit`.

Сертификаты уже находятся в репозитории. При необходимости их можно создать
заново:

```bash
./scripts/generate-certificates.sh
```

Для локальной учебной работы ключи сохранены в проекте по условию задания.
Эти сертификаты и пароль нельзя использовать в production.

## Пользователи и JAAS

Пользователи Kafka определены в
`jaas-config/kafka_server_jaas.conf`:

| Пользователь | Назначение |
|---|---|
| `admin` | межброкерное взаимодействие, управление топиками и ACL |
| `producer` | отправка сообщений |
| `consumer` | чтение разрешённых топиков |

Клиент передаёт имя и пароль через `sasl.jaas.config` в соответствующем
файле каталога `client/`. Kafka преобразует имя в principal вида
`User:producer` или `User:consumer`, после чего проверяет ACL.

На брокерах задан только один суперпользователь:

```properties
super.users=User:admin
```

Отдельная JAAS-конфигурация
`jaas-config/zookeeper_server_jaas.conf` защищает соединение брокеров с
ZooKeeper. Брокеры подключаются к нему под служебной учётной записью `kafka`,
а `zookeeper.set.acl=true` защищает созданные znode.

## Запуск

Из корня репозитория перейдите в каталог практической работы:

```bash
cd kafka/practical_work_6
```

Запустите инфраструктуру:

```bash
docker compose up -d
docker compose ps
```

AKHQ доступен по адресу [http://localhost:8080](http://localhost:8080).
Он подключается к Kafka по SASL_SSL как пользователь `admin`.

Проверьте TLS-соединение с первым брокером:

```bash
openssl s_client \
  -connect localhost:29092 \
  -servername localhost \
  -CAfile certs/ca/ca.crt \
  -verify_return_error \
  -brief </dev/null
```

В результате должны присутствовать строки `Protocol version: TLSv1.3`
и `Verification: OK`. Аналогично доступны брокеры на портах `29093`
и `29094`.

Создайте топики и ACL:

```bash
docker compose exec -T kafka-client /scripts/create-topics-and-acls.sh
```

Скрипт ожидает готовности Kafka, создаёт топики `topic-1` и `topic-2`
с тремя партициями и фактором репликации 3, после чего назначает права.

## Матрица доступа

| Клиент | topic-1 | topic-2 |
|---|---|---|
| producer | запись | запись |
| consumer | чтение | запрещено |
| admin | полный доступ | полный доступ |

На брокерах включены:

```properties
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:admin
```

Поэтому операция разрешена только при наличии подходящего ACL.

## Автоматическая проверка

Выполните:

```bash
docker compose exec -T kafka-client /scripts/test-access.sh
```

Скрипт:

1. отправляет сообщение продюсером в `topic-1`;
2. отправляет сообщение продюсером в `topic-2`;
3. успешно читает `topic-1` консьюмером;
4. получает ожидаемую ошибку авторизации при чтении `topic-2`.

Успешная проверка завершается строкой:

```text
Expected result: consumer access to topic-2 is denied.
```

## Ручная проверка

Отправка сообщения в `topic-1`:

```bash
docker compose exec kafka-client kafka-console-producer \
  --bootstrap-server kafka-1:9092 \
  --producer.config /etc/kafka/client/producer.properties \
  --topic topic-1
```

Чтение `topic-1`:

```bash
docker compose exec kafka-client kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --consumer.config /etc/kafka/client/consumer.properties \
  --topic topic-1 \
  --from-beginning
```

Попытка чтения `topic-2` тем же консьюмером должна завершиться
`TopicAuthorizationException`:

```bash
docker compose exec kafka-client kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --consumer.config /etc/kafka/client/consumer.properties \
  --topic topic-2 \
  --from-beginning
```

Просмотр ACL от имени администратора:

```bash
docker compose exec -T kafka-client kafka-acls \
  --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/client/admin.properties \
  --list
```

## Остановка

```bash
docker compose down
```

Чтобы также удалить данные Kafka:

```bash
docker compose down -v
```
