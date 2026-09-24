import gzip
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import tracker_worker as worker


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name) / "data"
        self.data.mkdir()
        self.db = self.data / "syscoin_tracker.sqlite"
        for name, value in (("DATA", self.data), ("DB", self.db)):
            active = patch.object(worker, name, value)
            active.start()
            self.addCleanup(active.stop)
        active = patch.object(worker.shutil, "disk_usage", return_value=SimpleNamespace(free=100 * 1024**3))
        active.start()
        self.addCleanup(active.stop)

    def seed(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO metadata VALUES ('last_summary', 'saved data')")

    def test_missing_source_is_not_created(self):
        with self.assertRaisesRegex(RuntimeError, "missing or empty"):
            worker.backup_database()
        self.assertFalse(self.db.exists())

    def test_valid_backup_can_be_restored(self):
        self.seed()
        result = worker.backup_database()
        archive = self.data.parent / "backups" / result["archive"]
        with gzip.open(archive, "rb") as source:
            restored = sqlite3.connect(":memory:")
            restored.deserialize(source.read())
        self.assertEqual(restored.execute("PRAGMA quick_check").fetchone()[0], "ok")
        self.assertEqual(restored.execute("SELECT value FROM metadata").fetchone()[0], "saved data")
        restored.close()

    def test_compression_failure_does_not_rotate_good_backups(self):
        self.seed()
        backups = self.data.parent / "backups"
        backups.mkdir()
        previous = []
        for number in range(3):
            path = backups / f"tracker-2000010{number + 1}.sqlite.gz"
            path.write_bytes(b"previous backup")
            previous.append(path)
        with patch.object(worker.gzip, "open", side_effect=OSError("disk failed")):
            with self.assertRaises(OSError):
                worker.backup_database()
        self.assertEqual(set(backups.iterdir()), set(previous))

    def test_live_wal_database_produces_standalone_archive_without_sidecars(self):
        self.seed()
        writer = sqlite3.connect(self.db)
        self.addCleanup(writer.close)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE metadata SET value='latest committed data'")
        writer.commit()
        result = worker.backup_database()
        backups = self.data.parent / "backups"
        archive = backups / result["archive"]
        self.assertEqual(list(backups.iterdir()), [archive])
        with gzip.open(archive, "rb") as source:
            restored = sqlite3.connect(":memory:")
            self.addCleanup(restored.close)
            restored.deserialize(source.read())
        self.assertEqual(restored.execute("PRAGMA quick_check").fetchone()[0], "ok")
        self.assertEqual(restored.execute("SELECT value FROM metadata").fetchone()[0], "latest committed data")

    def test_backup_refuses_insufficient_working_space(self):
        self.seed()
        with patch.object(worker.shutil, "disk_usage", return_value=SimpleNamespace(free=15 * 1024**3)):
            with self.assertRaisesRegex(RuntimeError, "Not enough free space"):
                worker.backup_database()
