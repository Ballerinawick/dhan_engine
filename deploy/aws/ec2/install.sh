#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update
sudo apt-get install -y docker.io git
sudo systemctl enable --now docker
sudo install -d -m 0750 /etc/dhan-engine /var/lib/dhan-engine
sudo install -m 0644 deploy/aws/ec2/dhan-engine.service /etc/systemd/system/dhan-engine.service
if [[ ! -f /etc/dhan-engine/dhan-engine.env ]]; then
  sudo install -m 0600 deploy/aws/ec2/dhan-engine.env.example /etc/dhan-engine/dhan-engine.env
fi
sudo docker build --build-arg DEEPLOB_INSTALL=inference -t dhan-engine:latest .
sudo systemctl daemon-reload
echo "Edit /etc/dhan-engine/dhan-engine.env, attach an S3 IAM role, then run:"
echo "sudo systemctl enable --now dhan-engine"

