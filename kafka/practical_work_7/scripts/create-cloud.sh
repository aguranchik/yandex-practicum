#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env

if [[ "${CONFIRM_PAID_RESOURCES:-}" != "YES" ]]; then
  cat >&2 <<EOF
This command creates billable Yandex Cloud resources:
  - 3 Kafka brokers: ${KAFKA_RESOURCE_PRESET}, ${KAFKA_DISK_SIZE_GB} GB ${KAFKA_DISK_TYPE} each
  - 1 preemptible VM: ${VM_CORES} vCPU (${VM_CORE_FRACTION}%), ${VM_MEMORY_GB} GB RAM, ${VM_DISK_SIZE_GB} GB HDD

Run it only after cost approval:
  CONFIRM_PAID_RESOURCES=YES scripts/create-cloud.sh
EOF
  exit 2
fi

"$PROJECT_DIR/scripts/preflight.sh"

for existing in \
  "$(resource_id 'managed-kafka cluster' "$KAFKA_CLUSTER_NAME")" \
  "$(resource_id 'compute instance' "$VM_NAME")"; do
  if [[ -n "$existing" ]]; then
    echo "A project resource already exists ($existing). Refusing a duplicate deployment." >&2
    exit 1
  fi
done

mkdir -p "$PROJECT_DIR/artifacts/logs"

if [[ -z "$(resource_id 'vpc network' "$NETWORK_NAME")" ]]; then
  yc_cmd vpc network create \
    --name "$NETWORK_NAME" \
    --description "Practical work 7 isolated network" \
    --labels project=practical-work-7
fi

if [[ -z "$(resource_id 'vpc subnet' "$SUBNET_A_NAME")" ]]; then
  yc_cmd vpc subnet create --name "$SUBNET_A_NAME" --network-name "$NETWORK_NAME" --zone ru-central1-a --range 10.70.1.0/24
fi
if [[ -z "$(resource_id 'vpc subnet' "$SUBNET_B_NAME")" ]]; then
  yc_cmd vpc subnet create --name "$SUBNET_B_NAME" --network-name "$NETWORK_NAME" --zone ru-central1-b --range 10.70.2.0/24
fi
if [[ -z "$(resource_id 'vpc subnet' "$SUBNET_D_NAME")" ]]; then
  yc_cmd vpc subnet create --name "$SUBNET_D_NAME" --network-name "$NETWORK_NAME" --zone ru-central1-d --range 10.70.3.0/24
fi

if [[ -z "$(resource_id 'vpc security-group' "$VM_SECURITY_GROUP_NAME")" ]]; then
  yc_cmd vpc security-group create \
    --name "$VM_SECURITY_GROUP_NAME" \
    --network-name "$NETWORK_NAME" \
    --description "SSH and private VPC access for the pw7 NiFi VM" \
    --rule "direction=ingress,protocol=tcp,port=22,v4-cidrs=[$ADMIN_CIDR]" \
    --rule "direction=ingress,protocol=any,port=any,v4-cidrs=[10.70.0.0/16]" \
    --rule "direction=egress,protocol=any,port=any,v4-cidrs=[0.0.0.0/0]"
fi

if [[ -z "$(resource_id 'vpc security-group' "$KAFKA_SECURITY_GROUP_NAME")" ]]; then
  yc_cmd vpc security-group create \
    --name "$KAFKA_SECURITY_GROUP_NAME" \
    --network-name "$NETWORK_NAME" \
    --description "Private VPC access for the pw7 Managed Kafka cluster" \
    --rule "direction=ingress,protocol=any,port=any,v4-cidrs=[10.70.0.0/16]" \
    --rule "direction=egress,protocol=any,port=any,v4-cidrs=[0.0.0.0/0]"
fi

vm_sg_id="$(resource_id 'vpc security-group' "$VM_SECURITY_GROUP_NAME")"
kafka_sg_id="$(resource_id 'vpc security-group' "$KAFKA_SECURITY_GROUP_NAME")"
subnet_a_id="$(resource_id 'vpc subnet' "$SUBNET_A_NAME")"
subnet_b_id="$(resource_id 'vpc subnet' "$SUBNET_B_NAME")"
subnet_d_id="$(resource_id 'vpc subnet' "$SUBNET_D_NAME")"
expanded_ssh_key_path="${SSH_PUBLIC_KEY_PATH/#\~/$HOME}"
"$PROJECT_DIR/scripts/render-cloud-init.sh" "$expanded_ssh_key_path"

yc_cmd compute instance create \
  --name "$VM_NAME" \
  --hostname "$VM_NAME" \
  --description "Preemptible Apache NiFi host for practical work 7" \
  --labels project=practical-work-7 \
  --zone "$VM_ZONE" \
  --platform "$VM_PLATFORM" \
  --cores "$VM_CORES" \
  --core-fraction "$VM_CORE_FRACTION" \
  --memory "$VM_MEMORY_GB" \
  --preemptible \
  --create-boot-disk "type=network-hdd,size=$VM_DISK_SIZE_GB,image-family=ubuntu-2404-lts,image-folder-id=standard-images,auto-delete=true" \
  --network-interface "subnet-id=$subnet_a_id,nat-ip-version=ipv4,security-group-ids=[$vm_sg_id]" \
  --metadata-from-file "user-data=$PROJECT_DIR/build/user-data.yaml" \
  --async \
  --format json >"$PROJECT_DIR/artifacts/logs/vm-create-operation.json"

yc_cmd managed-kafka cluster create \
  --name "$KAFKA_CLUSTER_NAME" \
  --description "Three-broker KRaft cluster for practical work 7" \
  --environment production \
  --labels project=practical-work-7 \
  --version "$KAFKA_VERSION" \
  --network-name "$NETWORK_NAME" \
  --subnet-ids "$subnet_a_id,$subnet_b_id,$subnet_d_id" \
  --zone-ids ru-central1-a,ru-central1-b,ru-central1-d \
  --brokers-count 1 \
  --resource-preset "$KAFKA_RESOURCE_PRESET" \
  --disk-size "$KAFKA_DISK_SIZE_GB" \
  --disk-type "$KAFKA_DISK_TYPE" \
  --security-group-ids "$kafka_sg_id" \
  --schema-registry \
  --compression-type zstd \
  --log-retention-ms 86400000 \
  --log-segment-bytes 134217728 \
  --num-partitions 3 \
  --default-replication-factor 3 \
  --async \
  --format json >"$PROJECT_DIR/artifacts/logs/kafka-create-operation.json"

vm_operation_id="$(jq -r '.id' "$PROJECT_DIR/artifacts/logs/vm-create-operation.json")"
kafka_operation_id="$(jq -r '.id' "$PROJECT_DIR/artifacts/logs/kafka-create-operation.json")"

echo "VM operation: $vm_operation_id"
echo "Kafka operation: $kafka_operation_id"
echo "Both resources are provisioning in parallel."

yc_cmd operation wait "$vm_operation_id"
yc_cmd operation wait "$kafka_operation_id"

echo "Cloud resources are ready. Run scripts/configure-cloud.sh immediately."
