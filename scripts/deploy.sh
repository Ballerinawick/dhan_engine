#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_DIR="/etc/dhan-engine"
DATA_DIR="/var/lib/dhan-engine"
ENV_FILE="${ENV_DIR}/dhan-engine.env"
SECRET_ID="trading-bot/dhan"
REGION="ap-south-1"

cd "${ROOT_DIR}"

# Install and start Docker on Amazon Linux 2023.
sudo dnf install -y docker jq
sudo systemctl enable --now docker

# Create application directories.
sudo install -d -m 0750 "${ENV_DIR}" "${DATA_DIR}" "${DATA_DIR}/models"

# Retrieve Dhan credentials using the EC2 IAM role.
SECRET_JSON="$(aws secretsmanager get-secret-value \
  --region "${REGION}" \
  --secret-id "${SECRET_ID}" \
  --query SecretString \
  --output text)"

DHAN_CLIENT_ID="$(printf '%s' "${SECRET_JSON}" | jq -r '.DHAN_CLIENT_ID // empty')"
DHAN_ACCESS_TOKEN="$(printf '%s' "${SECRET_JSON}" | jq -r '.DHAN_ACCESS_TOKEN // empty')"

if [[ -z "${DHAN_CLIENT_ID}" || -z "${DHAN_ACCESS_TOKEN}" ]]; then
  echo "DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN is missing from Secrets Manager."
  exit 1
fi

# Generate the private runtime environment file.
TEMP_ENV="$(mktemp)"
trap 'rm -f "${TEMP_ENV}"' EXIT

grep -vE '^(DHAN_CLIENT_ID|DHAN_ACCESS_TOKEN)=' \
  deploy/aws/ec2/dhan-engine.env.example > "${TEMP_ENV}"

{
  printf 'DHAN_CLIENT_ID=%s\n' "${DHAN_CLIENT_ID}"
  printf 'DHAN_ACCESS_TOKEN=%s\n' "${DHAN_ACCESS_TOKEN}"
} >> "${TEMP_ENV}"

sudo install -m 0600 "${TEMP_ENV}" "${ENV_FILE}"

# Retrieve the versioned production model before replacing the running service.
# Atomic moves prevent a partially downloaded artifact from being loaded.
MODEL_S3_URI="$(grep '^DEEPLOB_MODEL_S3_URI=' "${TEMP_ENV}" | cut -d= -f2-)"
METADATA_S3_URI="$(grep '^DEEPLOB_METADATA_S3_URI=' "${TEMP_ENV}" | cut -d= -f2-)"
if [[ -z "${MODEL_S3_URI}" || -z "${METADATA_S3_URI}" ]]; then
  echo "DeepLOB model S3 URIs are required for the combined live service."
  exit 1
fi
MODEL_TEMP="$(mktemp)"
METADATA_TEMP="$(mktemp)"
trap 'rm -f "${TEMP_ENV}" "${MODEL_TEMP}" "${METADATA_TEMP}"' EXIT
aws s3 cp "${MODEL_S3_URI}" "${MODEL_TEMP}"
aws s3 cp "${METADATA_S3_URI}" "${METADATA_TEMP}"
sudo install -m 0640 "${MODEL_TEMP}" "${DATA_DIR}/models/deeplob.pt"
sudo install -m 0640 "${METADATA_TEMP}" "${DATA_DIR}/models/deeplob.json"

# Install the systemd service. The runtime downloads a fresh instrument master
# atomically at every start so an expired repository snapshot is never seeded.
sudo install -m 0644 deploy/aws/ec2/dhan-engine.service \
  /etc/systemd/system/dhan-engine.service

# Build the latest application image.
sudo docker build \
  --build-arg DEEPLOB_INSTALL=inference \
  -t dhan-engine:latest .

# Start or restart the trading service.
sudo systemctl daemon-reload
sudo systemctl enable dhan-engine
sudo systemctl restart dhan-engine
sudo systemctl --no-pager --full status dhan-engine

