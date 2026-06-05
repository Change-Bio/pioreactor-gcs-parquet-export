#!/bin/bash
# LEADER_ONLY
# Stop and disable the startup unit on uninstall.
set -a
source /etc/pioreactor.env
set +a

sudo systemctl stop    pioreactor_startup_run@gcs_parquet_export.service || true
sudo systemctl disable pioreactor_startup_run@gcs_parquet_export.service || true

echo "gcs_parquet_export: disabled startup unit"
