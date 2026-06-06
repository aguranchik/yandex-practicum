#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CERTS_DIR="${PROJECT_DIR}/certs"
PASSWORD="${STORE_PASSWORD:-changeit}"
VALIDITY_DAYS="${VALIDITY_DAYS:-3650}"

rm -rf "${CERTS_DIR}"
mkdir -p "${CERTS_DIR}/ca" "${CERTS_DIR}/client"

openssl req \
  -new \
  -newkey rsa:4096 \
  -x509 \
  -nodes \
  -days "${VALIDITY_DAYS}" \
  -subj "/CN=practical-work-6-ca" \
  -keyout "${CERTS_DIR}/ca/ca.key" \
  -out "${CERTS_DIR}/ca/ca.crt"

create_identity() {
  local identity="$1"
  local output_dir="$2"
  local usage="$3"
  local san="$4"

  mkdir -p "${output_dir}"

  openssl genrsa -out "${output_dir}/${identity}.key" 2048

  openssl req \
    -new \
    -key "${output_dir}/${identity}.key" \
    -subj "/CN=${identity}" \
    -out "${output_dir}/${identity}.csr"

  {
    echo "basicConstraints=CA:FALSE"
    echo "keyUsage=digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=${usage}"
    if [[ -n "${san}" ]]; then
      echo "subjectAltName=${san}"
    fi
  } > "${output_dir}/${identity}.ext"

  openssl x509 \
    -req \
    -in "${output_dir}/${identity}.csr" \
    -CA "${CERTS_DIR}/ca/ca.crt" \
    -CAkey "${CERTS_DIR}/ca/ca.key" \
    -CAcreateserial \
    -days "${VALIDITY_DAYS}" \
    -sha256 \
    -extfile "${output_dir}/${identity}.ext" \
    -out "${output_dir}/${identity}.crt"

  openssl pkcs12 \
    -export \
    -name "${identity}" \
    -in "${output_dir}/${identity}.crt" \
    -inkey "${output_dir}/${identity}.key" \
    -certfile "${CERTS_DIR}/ca/ca.crt" \
    -password "pass:${PASSWORD}" \
    -out "${output_dir}/${identity}.p12"

  keytool -importkeystore \
    -srckeystore "${output_dir}/${identity}.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass "${PASSWORD}" \
    -destkeystore "${output_dir}/${identity}.keystore.jks" \
    -deststoretype JKS \
    -deststorepass "${PASSWORD}" \
    -destkeypass "${PASSWORD}" \
    -noprompt

  keytool -importcert \
    -alias practical-work-6-ca \
    -file "${CERTS_DIR}/ca/ca.crt" \
    -keystore "${output_dir}/${identity}.truststore.jks" \
    -storepass "${PASSWORD}" \
    -noprompt

  cp "${CERTS_DIR}/ca/ca.crt" "${output_dir}/ca.crt"
  printf '%s' "${PASSWORD}" > "${output_dir}/keystore_creds"
  printf '%s' "${PASSWORD}" > "${output_dir}/key_creds"
  printf '%s' "${PASSWORD}" > "${output_dir}/truststore_creds"
}

for broker in kafka-1 kafka-2 kafka-3; do
  create_identity \
    "${broker}" \
    "${CERTS_DIR}/${broker}" \
    "serverAuth,clientAuth" \
    "DNS:${broker},DNS:localhost,IP:127.0.0.1"
done

keytool -importcert \
  -alias practical-work-6-ca \
  -file "${CERTS_DIR}/ca/ca.crt" \
  -keystore "${CERTS_DIR}/client/kafka.truststore.jks" \
  -storepass "${PASSWORD}" \
  -noprompt

printf '%s' "${PASSWORD}" > "${CERTS_DIR}/client/truststore_creds"

chmod 600 "${CERTS_DIR}/ca/ca.key"
find "${CERTS_DIR}" -name "*.key" -exec chmod 600 {} \;

echo "Certificates generated in ${CERTS_DIR}"
