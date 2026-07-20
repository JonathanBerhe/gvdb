"""Unit tests for GVDBClient backup/restore methods. No server required.

The gRPC stub is replaced with a MagicMock, so tests exercise argument
validation, request field mapping, and response conversion only.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from gvdb import BackupInfo, BackupStatus, GVDBClient, RestoreStatus
from gvdb.pb import vectordb_pb2 as pb


@pytest.fixture
def offline_client():
    """GVDBClient with the gRPC stub replaced by a MagicMock."""
    c = GVDBClient("localhost:1")
    c._stub = MagicMock()
    yield c
    c.close()


def _backup_status_response(state, **overrides):
    defaults = {
        "backup_id": "bk-1",
        "state": state,
        "shards_total": 4,
        "shards_completed": 4,
        "bytes_uploaded": 2048,
        "error_message": "",
        "elapsed_seconds": 1.5,
        "manifest_uri": "",
    }
    defaults.update(overrides)
    return pb.GetBackupStatusResponse(**defaults)


def _restore_status_response(state, **overrides):
    defaults = {
        "restore_id": "rs-1",
        "state": state,
        "shards_total": 2,
        "shards_completed": 2,
        "error_message": "",
        "elapsed_seconds": 0.5,
    }
    defaults.update(overrides)
    return pb.GetRestoreStatusResponse(**defaults)


# ---------------------------------------------------------------------------
# Target validation
# ---------------------------------------------------------------------------


class TestTargetValidation:
    def test_backup_requires_a_target(self, offline_client):
        with pytest.raises(ValueError, match="exactly one"):
            offline_client.backup_collection("docs")
        offline_client._stub.BackupCollection.assert_not_called()

    def test_backup_rejects_both_targets(self, offline_client):
        with pytest.raises(ValueError, match="exactly one"):
            offline_client.backup_collection(
                "docs", s3_bucket="bkt", local_path="/backups"
            )
        offline_client._stub.BackupCollection.assert_not_called()

    def test_backup_prefix_requires_bucket(self, offline_client):
        with pytest.raises(ValueError, match="s3_prefix requires s3_bucket"):
            offline_client.backup_collection(
                "docs", local_path="/backups", s3_prefix="prod"
            )
        offline_client._stub.BackupCollection.assert_not_called()

    def test_restore_requires_a_target(self, offline_client):
        with pytest.raises(ValueError, match="exactly one"):
            offline_client.restore_collection("bk-1")
        offline_client._stub.RestoreCollection.assert_not_called()

    def test_restore_rejects_both_targets(self, offline_client):
        with pytest.raises(ValueError, match="exactly one"):
            offline_client.restore_collection(
                "bk-1", s3_bucket="bkt", local_path="/backups"
            )
        offline_client._stub.RestoreCollection.assert_not_called()

    def test_list_backups_requires_a_target(self, offline_client):
        with pytest.raises(ValueError, match="exactly one"):
            offline_client.list_backups()
        offline_client._stub.ListBackups.assert_not_called()


# ---------------------------------------------------------------------------
# Request field mapping
# ---------------------------------------------------------------------------


class TestRequestMapping:
    def test_backup_s3_target(self, offline_client):
        stub = offline_client._stub
        stub.BackupCollection.return_value = pb.BackupCollectionResponse(
            backup_id="bk-42"
        )

        backup_id = offline_client.backup_collection(
            "docs", s3_bucket="bkt", s3_prefix="prod", backup_id="bk-42"
        )

        assert backup_id == "bk-42"
        request = stub.BackupCollection.call_args[0][0]
        assert request.collection_name == "docs"
        assert request.target.WhichOneof("target") == "s3"
        assert request.target.s3.bucket == "bkt"
        assert request.target.s3.prefix == "prod"
        assert request.backup_id == "bk-42"

    def test_backup_local_target(self, offline_client):
        stub = offline_client._stub
        stub.BackupCollection.return_value = pb.BackupCollectionResponse(
            backup_id="bk-1"
        )

        offline_client.backup_collection("docs", local_path="/backups")

        request = stub.BackupCollection.call_args[0][0]
        assert request.target.WhichOneof("target") == "local"
        assert request.target.local.path == "/backups"
        # No client-supplied ID: the server allocates one
        assert request.backup_id == ""

    def test_restore_s3_target(self, offline_client):
        stub = offline_client._stub
        stub.RestoreCollection.return_value = pb.RestoreCollectionResponse(
            restore_id="rs-7"
        )

        restore_id = offline_client.restore_collection(
            "bk-1",
            s3_bucket="bkt",
            s3_prefix="prod",
            target_collection="docs_copy",
            overwrite=True,
        )

        assert restore_id == "rs-7"
        request = stub.RestoreCollection.call_args[0][0]
        assert request.backup_id == "bk-1"
        assert request.source.WhichOneof("target") == "s3"
        assert request.source.s3.bucket == "bkt"
        assert request.source.s3.prefix == "prod"
        assert request.target_collection_name == "docs_copy"
        assert request.overwrite is True

    def test_restore_local_target_defaults(self, offline_client):
        stub = offline_client._stub
        stub.RestoreCollection.return_value = pb.RestoreCollectionResponse(
            restore_id="rs-1"
        )

        offline_client.restore_collection("bk-1", local_path="/backups")

        request = stub.RestoreCollection.call_args[0][0]
        assert request.source.WhichOneof("target") == "local"
        assert request.source.local.path == "/backups"
        # Empty target name: the server uses the manifest's collection name
        assert request.target_collection_name == ""
        assert request.overwrite is False

    def test_list_backups_maps_result(self, offline_client):
        stub = offline_client._stub
        stub.ListBackups.return_value = pb.ListBackupsResponse(
            backups=[
                pb.BackupInfo(
                    backup_id="bk-1",
                    collection_name="docs",
                    collection_id=3,
                    created_at_unix_ms=1700000000000,
                    vector_count=1000,
                    size_bytes=4096,
                    manifest_version=1,
                )
            ]
        )

        backups = offline_client.list_backups(s3_bucket="bkt")

        request = stub.ListBackups.call_args[0][0]
        assert request.source.s3.bucket == "bkt"
        assert backups == [
            BackupInfo(
                backup_id="bk-1",
                collection_name="docs",
                collection_id=3,
                created_at_unix_ms=1700000000000,
                vector_count=1000,
                size_bytes=4096,
                manifest_version=1,
            )
        ]

    def test_get_backup_status_maps_fields(self, offline_client):
        stub = offline_client._stub
        stub.GetBackupStatus.return_value = _backup_status_response(
            pb.BACKUP_COMPLETED, manifest_uri="s3://bkt/backups/bk-1/manifest.json"
        )

        status = offline_client.get_backup_status("bk-1")

        request = stub.GetBackupStatus.call_args[0][0]
        assert request.backup_id == "bk-1"
        assert status == BackupStatus(
            backup_id="bk-1",
            state="BACKUP_COMPLETED",
            shards_total=4,
            shards_completed=4,
            bytes_uploaded=2048,
            error_message="",
            elapsed_seconds=1.5,
            manifest_uri="s3://bkt/backups/bk-1/manifest.json",
        )

    def test_get_restore_status_maps_fields(self, offline_client):
        stub = offline_client._stub
        stub.GetRestoreStatus.return_value = _restore_status_response(
            pb.BACKUP_RUNNING, shards_completed=1
        )

        status = offline_client.get_restore_status("rs-1")

        request = stub.GetRestoreStatus.call_args[0][0]
        assert request.restore_id == "rs-1"
        assert status == RestoreStatus(
            restore_id="rs-1",
            state="BACKUP_RUNNING",
            shards_total=2,
            shards_completed=1,
            error_message="",
            elapsed_seconds=0.5,
        )

    def test_cancel_backup(self, offline_client):
        stub = offline_client._stub
        stub.CancelBackup.return_value = pb.CancelBackupResponse(success=True)

        assert offline_client.cancel_backup("bk-1") is True
        request = stub.CancelBackup.call_args[0][0]
        assert request.backup_id == "bk-1"


# ---------------------------------------------------------------------------
# wait_for_backup / wait_for_restore
# ---------------------------------------------------------------------------


class TestWaitForBackup:
    def test_completed_returns_status(self, offline_client):
        stub = offline_client._stub
        stub.GetBackupStatus.side_effect = [
            _backup_status_response(pb.BACKUP_RUNNING, shards_completed=1),
            _backup_status_response(
                pb.BACKUP_COMPLETED, manifest_uri="s3://bkt/m.json"
            ),
        ]

        status = offline_client.wait_for_backup(
            "bk-1", poll_interval=0.001, timeout=5.0
        )

        assert status.state == "BACKUP_COMPLETED"
        assert status.manifest_uri == "s3://bkt/m.json"
        assert stub.GetBackupStatus.call_count == 2

    def test_failed_raises(self, offline_client):
        offline_client._stub.GetBackupStatus.return_value = _backup_status_response(
            pb.BACKUP_FAILED, error_message="disk full"
        )

        with pytest.raises(RuntimeError, match="BACKUP_FAILED.*disk full"):
            offline_client.wait_for_backup("bk-1", poll_interval=0.001, timeout=5.0)

    def test_cancelled_raises(self, offline_client):
        offline_client._stub.GetBackupStatus.return_value = _backup_status_response(
            pb.BACKUP_CANCELLED
        )

        with pytest.raises(RuntimeError, match="BACKUP_CANCELLED"):
            offline_client.wait_for_backup("bk-1", poll_interval=0.001, timeout=5.0)

    def test_timeout_raises(self, offline_client):
        offline_client._stub.GetBackupStatus.return_value = _backup_status_response(
            pb.BACKUP_RUNNING, shards_completed=1
        )

        with pytest.raises(TimeoutError, match="bk-1 did not complete"):
            offline_client.wait_for_backup("bk-1", poll_interval=0.001, timeout=0.02)


class TestWaitForRestore:
    def test_completed_returns_status(self, offline_client):
        offline_client._stub.GetRestoreStatus.return_value = _restore_status_response(
            pb.BACKUP_COMPLETED
        )

        status = offline_client.wait_for_restore(
            "rs-1", poll_interval=0.001, timeout=5.0
        )
        assert status.state == "BACKUP_COMPLETED"

    def test_failed_raises(self, offline_client):
        offline_client._stub.GetRestoreStatus.return_value = _restore_status_response(
            pb.BACKUP_FAILED, error_message="manifest missing"
        )

        with pytest.raises(RuntimeError, match="BACKUP_FAILED.*manifest missing"):
            offline_client.wait_for_restore("rs-1", poll_interval=0.001, timeout=5.0)

    def test_timeout_raises(self, offline_client):
        offline_client._stub.GetRestoreStatus.return_value = _restore_status_response(
            pb.BACKUP_RUNNING, shards_completed=0
        )

        with pytest.raises(TimeoutError, match="rs-1 did not complete"):
            offline_client.wait_for_restore("rs-1", poll_interval=0.001, timeout=0.02)
