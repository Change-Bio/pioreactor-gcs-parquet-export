# -*- coding: utf-8 -*-
"""
gcs_parquet_export — leader-only Pioreactor background job.

On leader startup and then every `sync_interval_seconds`, incrementally exports the
leader's SQLite tables to Parquet in a GCS bucket, partitioned by experiment. All
parameters live in `[gcs_parquet_export.config]` in config.ini.

The heavy lifting is in `exporter.sync_once`; this class only wires config + a
RepeatedTimer + MQTT status fields, and guards against overlapping runs.
"""
from datetime import datetime, timezone

import click
from pioreactor.background_jobs.base import BackgroundJob
from pioreactor.config import config
from pioreactor.utils.timing import RepeatedTimer
from pioreactor.whoami import get_unit_name, UNIVERSAL_EXPERIMENT

from pioreactor_gcs_parquet_export import exporter


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _csv_list(raw: str) -> list:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


class GcsParquetExport(BackgroundJob):
    """Periodically sync SQLite -> Parquet in GCS, partitioned by experiment."""

    job_name = "gcs_parquet_export"

    published_settings = {
        "sync_interval_seconds": {"datatype": "integer", "settable": True, "unit": "s"},
        "last_sync_started_at": {"datatype": "string", "settable": False},
        "last_sync_finished_at": {"datatype": "string", "settable": False},
        "last_sync_status": {"datatype": "string", "settable": False},
        "last_error": {"datatype": "string", "settable": False},
        "rows_uploaded_last_cycle": {"datatype": "integer", "settable": False},
        "files_uploaded_last_cycle": {"datatype": "integer", "settable": False},
    }

    def __init__(self, sync_interval_seconds: int, unit: str, experiment: str, **kwargs):
        super().__init__(unit=unit, experiment=experiment)

        section = f"{self.job_name}.config"
        self.cfg = {
            "bucket": config.get(section, "bucket"),
            "prefix": config.get(section, "prefix", fallback=""),
            "data_tables": config.get(section, "data_tables", fallback="auto"),
            "exclude": _csv_list(config.get(section, "exclude", fallback="")),
            "meta_tables": _csv_list(config.get(section, "meta_tables", fallback="")),
            "gcloud_path": config.get(
                section, "gcloud_path", fallback="/home/pioreactor/google-cloud-sdk/bin/gcloud"
            ),
            "gcp_project": config.get(section, "gcp_project", fallback=""),
            "staging_dir": config.get(section, "staging_dir", fallback="/tmp/pioreactor_gcs_export"),
            "batch_rows": config.getint(section, "batch_rows", fallback=200000),
            "db_path": config.get(
                section, "db_path", fallback="/home/pioreactor/.pioreactor/storage/pioreactor.sqlite"
            ),
            "state_path": config.get(
                section,
                "state_path",
                fallback="/home/pioreactor/.pioreactor/storage/gcs_export_state.json",
            ),
        }

        # published settings / status
        self.sync_interval_seconds = int(sync_interval_seconds)
        self.last_sync_started_at = ""
        self.last_sync_finished_at = ""
        self.last_sync_status = "never"
        self.last_error = ""
        self.rows_uploaded_last_cycle = 0
        self.files_uploaded_last_cycle = 0

        self._busy = False
        # run_immediately=True => one sync on startup, then every interval.
        self.timer = RepeatedTimer(
            self.sync_interval_seconds, self._sync, job_name=self.job_name, run_immediately=True
        ).start()

    # --------------------------------------------------------------------- sync

    def _log(self, level: str, msg: str) -> None:
        getattr(self.logger, level, self.logger.info)(msg)

    def _sync(self) -> None:
        if self.state != self.READY or self._busy:
            return
        self._busy = True
        self.last_sync_started_at = _now_iso()
        try:
            stats = exporter.sync_once(
                self.cfg,
                log=self._log,
                should_continue=lambda: self.state == self.READY,
            )
            self.rows_uploaded_last_cycle = stats["rows_uploaded"]
            self.files_uploaded_last_cycle = stats["files_uploaded"]
            if stats["errors"]:
                self.last_sync_status = f"completed with {len(stats['errors'])} error(s)"
                self.last_error = stats["errors"][-1][:500]
                self.logger.warning(
                    f"sync finished with {len(stats['errors'])} error(s); "
                    f"{stats['files_uploaded']} files, {stats['rows_uploaded']} rows uploaded"
                )
            else:
                self.last_sync_status = "ok"
                self.last_error = ""
                self.logger.info(
                    f"sync ok: {stats['files_uploaded']} files, "
                    f"{stats['rows_uploaded']} rows uploaded"
                )
        except Exception as e:  # noqa: BLE001 — never let a sync crash the job
            self.last_sync_status = "error"
            self.last_error = str(e)[:500]
            self.logger.error(f"sync failed: {e}", exc_info=True)
        finally:
            self.last_sync_finished_at = _now_iso()
            self._busy = False

    # --------------------------------------------------------------------- settings

    def set_sync_interval_seconds(self, value) -> None:
        self.sync_interval_seconds = int(value)
        if hasattr(self, "timer"):
            self.timer.cancel()
        self.timer = RepeatedTimer(
            self.sync_interval_seconds, self._sync, job_name=self.job_name, run_immediately=False
        ).start()
        self.logger.info(f"sync_interval_seconds set to {self.sync_interval_seconds}")

    # --------------------------------------------------------------------- lifecycle

    def on_disconnected(self):
        super().on_disconnected()
        if hasattr(self, "timer"):
            self.timer.cancel()


@click.command(name="gcs_parquet_export")
@click.option(
    "--sync-interval-seconds",
    default=lambda: config.getint("gcs_parquet_export.config", "sync_interval_seconds", fallback=3600),
    type=int,
    show_default="config.ini",
    help="Seconds between syncs (first sync runs immediately on start).",
)
def click_gcs_parquet_export(sync_interval_seconds):
    """Start the GCS Parquet export job (leader only)."""
    # Leader-only infra job, not tied to a single experiment — use the universal
    # sentinel (same as Monitor) so it runs without an experiment assignment.
    job = GcsParquetExport(
        sync_interval_seconds=sync_interval_seconds,
        unit=get_unit_name(),
        experiment=UNIVERSAL_EXPERIMENT,
    )
    job.block_until_disconnected()


if __name__ == "__main__":
    click_gcs_parquet_export()
