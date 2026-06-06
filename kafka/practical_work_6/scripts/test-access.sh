#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka-1:9092}"
PRODUCER_CONFIG="/etc/kafka/client/producer.properties"
CONSUMER_CONFIG="/etc/kafka/client/consumer.properties"

printf 'encrypted message for topic-1\n' | kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --producer.config "${PRODUCER_CONFIG}" \
  --topic topic-1

printf 'encrypted message for topic-2\n' | kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --producer.config "${PRODUCER_CONFIG}" \
  --topic topic-2

echo "Reading topic-1 as consumer:"
kafka-console-consumer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --consumer.config "${CONSUMER_CONFIG}" \
  --topic topic-1 \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000

echo
echo "Reading topic-2 as consumer must be denied:"
set +e
topic_2_output="$(
  kafka-console-consumer \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --consumer.config "${CONSUMER_CONFIG}" \
    --topic topic-2 \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 10000 2>&1
)"
topic_2_status=$?
set -e

echo "${topic_2_output}"

if grep -Eqi "authorization|not authorized|TopicAuthorizationException" <<< "${topic_2_output}"; then
  echo "Expected result: consumer access to topic-2 is denied."
  exit 0
fi

echo "Expected an authorization error for topic-2, got exit code ${topic_2_status}." >&2
exit 1
