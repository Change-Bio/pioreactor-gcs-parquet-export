# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name="pioreactor-gcs-parquet-export",
    version="0.1.0",
    license_files=('LICENSE.txt',),
    description="Export Pioreactor SQLite data to Parquet in a GCS bucket, partitioned by experiment, incrementally",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Noah",
    author_email="noah@changebio.uk",
    url="https://github.com/Change-Bio/pioreactor-gcs-parquet-export",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "pioreactor_gcs_parquet_export": [
            "additional_config.ini",
            "ui/contrib/jobs/*.yaml",
            "LEADER_ONLY",
            "post_install.sh",
            "pre_uninstall.sh",
        ]
    },
    install_requires=[
        "duckdb>=0.10",
    ],
    entry_points={
        "pioreactor.plugins": "pioreactor_gcs_parquet_export = pioreactor_gcs_parquet_export"
    },
)
