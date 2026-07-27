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
MODEL_TEMP=""
METADATA_TEMP=""
cleanup() {
  rm -f "${TEMP_ENV}"
  [[ -z "${MODEL_TEMP}" ]] || rm -f "${MODEL_TEMP}"
  [[ -z "${METADATA_TEMP}" ]] || rm -f "${METADATA_TEMP}"
}
trap cleanup EXIT

grep -vE '^(DHAN_CLIENT_ID|DHAN_ACCESS_TOKEN)=' \
  deploy/aws/ec2/dhan-engine.env.example > "${TEMP_ENV}"

{
  printf 'DHAN_CLIENT_ID=%s\n' "${DHAN_CLIENT_ID}"
  printf 'DHAN_ACCESS_TOKEN=%s\n' "${DHAN_ACCESS_TOKEN}"
} >> "${TEMP_ENV}"

sudo install -m 0600 "${TEMP_ENV}" "${ENV_FILE}"

DHAN_SERVICE="$(grep '^DHAN_SERVICE=' "${TEMP_ENV}" | cut -d= -f2- | tr '[:upper:]' '[:lower:]')"
DEEPLOB_INSTALL="recorder"

case "${DHAN_SERVICE}" in
  deeplob-live|deeplob_live|deeplob-inference|deeplob_inference)
    # Live and inference services fail closed unless both versioned model
    # artifacts can be downloaded before the running service is replaced.
    DEEPLOB_INSTALL="inference"
    MODEL_S3_URI="$(grep '^DEEPLOB_MODEL_S3_URI=' "${TEMP_ENV}" | cut -d= -f2-)"
    METADATA_S3_URI="$(grep '^DEEPLOB_METADATA_S3_URI=' "${TEMP_ENV}" | cut -d= -f2-)"
    if [[ -z "${MODEL_S3_URI}" || -z "${METADATA_S3_URI}" ]]; then
      echo "DeepLOB model S3 URIs are required for ${DHAN_SERVICE}."
      exit 1
    fi
    MODEL_TEMP="$(mktemp)"
    METADATA_TEMP="$(mktemp)"
    aws s3 cp "${MODEL_S3_URI}" "${MODEL_TEMP}"
    aws s3 cp "${METADATA_S3_URI}" "${METADATA_TEMP}"
    sudo install -m 0640 "${MODEL_TEMP}" "${DATA_DIR}/models/deeplob.pt"
    sudo install -m 0640 "${METADATA_TEMP}" "${DATA_DIR}/models/deeplob.json"
    echo "DEEPLOB_MODEL_DOWNLOAD_OK | service=${DHAN_SERVICE}"
    ;;
  deeplob-recorder|deeplob_recorder)
    # Data collection must not depend on a model that has not been trained yet.
    echo "DEEPLOB_MODEL_DOWNLOAD_SKIPPED | service=${DHAN_SERVICE} | reason=recorder_mode"
    ;;
  *)
    # Preserve the inference-capable image used by the existing non-DeepLOB
    # services while avoiding an unrelated model download requirement.
    DEEPLOB_INSTALL="inference"
    echo "DEEPLOB_MODEL_DOWNLOAD_SKIPPED | service=${DHAN_SERVICE} | reason=model_not_required"
    ;;
esac

# Install the systemd service. The runtime downloads a fresh instrument master
# atomically at every start so an expired repository snapshot is never seeded.
sudo install -m 0644 deploy/aws/ec2/dhan-engine.service \
  /etc/systemd/system/dhan-engine.service

# Build the latest application image with only the dependencies the service needs.
sudo docker build \
  --build-arg "DEEPLOB_INSTALL=${DEEPLOB_INSTALL}" \
  -t dhan-engine:latest .

# Start or restart the trading service.
sudo systemctl daemon-reload
sudo systemctl enable dhan-engine
sudo systemctl restart dhan-engine
sudo systemctl --no-pager --full status dhan-engine
