# Настройка Apache NiFi

NiFi запускается на VM `pw7-nifi` в Docker. UI не публикуется в интернет и
доступен только через SSH-туннель:

```bash
ssh -L 8087:127.0.0.1:8080 yc-user@<VM_PUBLIC_IP>
```

После запуска откройте `http://localhost:8087/nifi` и создайте поток:

```text
GenerateFlowFile -> PublishKafka_2_6 -> terminate success
```

## GenerateFlowFile

- Run Schedule: `10 sec`
- Custom Text:

```json
{"source":"nifi","message":"Practical work 7 NiFi event"}
```

- Unique FlowFiles: `false` — NiFi требует это значение при использовании
  Custom Text.

## SSL Context Service

Создайте `StandardSSLContextService`:

- Truststore Filename: `/opt/pw7/certs/kafka.truststore.jks`
- Truststore Password: `changeit`
- Truststore Type: `JKS`
- TLS Protocol: `TLS`

## PublishKafka_2_6

- Kafka Brokers: значение `KAFKA_BROKERS` из `/opt/pw7/.env`
- Topic Name: `pw7-nifi`
- Delivery Guarantee: `Guarantee Replicated Delivery`
- Compression Type: `lz4`
- Security Protocol: `SASL_SSL`
- SASL Mechanism: `SCRAM-SHA-512`
- Username: значение `KAFKA_USERNAME`
- Password: значение `KAFKA_PASSWORD`
- SSL Context Service: созданный `StandardSSLContextService`

Пароль не помещается в Git. После настройки flow definition будет выгружен в
`artifacts/` без секретного Parameter Context.
