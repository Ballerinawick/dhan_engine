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
sudo install -d -m 0750 "${ENV_DIR}" "${DATA_DIR}"

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

# Copy required instrument data and install the systemd service.
sudo install -m 0644 data/api-scrip-master.csv \
  "${DATA_DIR}/api-scrip-master.csv"

sudo install -m 0644 deploy/aws/ec2/dhan-engine.service \
  /etc/systemd/system/dhan-engine.service

# Build the latest application image.
sudo docker build \
  --build-arg DEEPLOB_INSTALL=recorder \
  -t dhan-engine:latest .

# Start or restart the trading service.
sudo systemctl daemon-reload
sudo systemctl enable dhan-engine
sudo systemctl restart dhan-engine
sudo systemctl --no-pager --full status dhan-engine
