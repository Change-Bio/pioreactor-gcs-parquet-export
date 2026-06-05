# -*- coding: utf-8 -*-
"""
pioreactor_gcs_parquet_export - Pioreactor plugin

Exports the leader's SQLite data to Parquet in a GCS bucket, partitioned by
experiment, incrementally, on a schedule. Leader-only.
"""

__version__ = "0.1.0"
__plugin_summary__ = "Export Pioreactor SQLite data to Parquet in a GCS bucket, partitioned by experiment"
__plugin_name__ = "pioreactor_gcs_parquet_export"
__plugin_author__ = "Noah"
__plugin_homepage__ = "https://github.com/Change-Bio/pioreactor-gcs-parquet-export"

# Importing the job registers its CLI command with Pioreactor. Guarded so the pure
# `exporter` module remains importable off-reactor (tests, static analysis) where
# `pioreactor`/`click` aren't installed.
try:
    from pioreactor_gcs_parquet_export.gcs_parquet_export import click_gcs_parquet_export
except ImportError:
    pass
