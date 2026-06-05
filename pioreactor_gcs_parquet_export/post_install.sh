#!/bin/bash
# LEADER_ONLY
# Enable the export job to start automatically on leader boot.
set -a
source /etc/pioreactor.env
set +a

sudo systemctl enable pioreactor_startup_run@gcs_parquet_export.service || true
sudo systemctl start  pioreactor_startup_run@gcs_parquet_export.service || true

echo "gcs_parquet_export: enabled startup unit pioreactor_startup_run@gcs_parquet_export.service"
