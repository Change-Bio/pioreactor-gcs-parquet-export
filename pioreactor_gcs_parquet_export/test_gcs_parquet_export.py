# -*- coding: utf-8 -*-
"""
On-reactor tests for the BackgroundJob wrapper. These need `pioreactor` installed
(run on a Pioreactor / in the pioreactor venv). The core export logic is tested
off-reactor in test_exporter.py.
"""
import pytest

pytest.importorskip("pioreactor")


def test_imports_and_registers_cli():
    from pioreactor_gcs_parquet_export import click_gcs_parquet_export
    from pioreactor_gcs_parquet_export.gcs_parquet_export import GcsParquetExport

    assert GcsParquetExport.job_name == "gcs_parquet_export"
    assert click_gcs_parquet_export.name == "gcs_parquet_export"
    # every settable published setting must have a matching setter
    for key, spec in GcsParquetExport.published_settings.items():
        if spec.get("settable"):
            assert hasattr(GcsParquetExport, f"set_{key}"), f"missing setter for {key}"
