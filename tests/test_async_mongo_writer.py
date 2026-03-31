"""
Tests for AsyncMongoWriter — background MongoDB writer.

Covers:
  - Normal write + flush
  - flush after close (no hang)
  - double close safety
  - Error propagation: flush raises on insert failure
  - Error recovery: second flush after error is clean (if no new errors)
  - __call__ after close

Requires:
  MongoDB accessible at MONGO_HOST (default: localhost).
  Set MONGO_HOST env var if MongoDB is remote.

Skip with: pytest -k "not mongo"
"""

import os
import time
import uuid

import pytest
from pymongo import MongoClient

from bluesky_dataforge import AsyncMongoWriter

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_URI = f"mongodb://{MONGO_HOST}:27017"
DB_NAME = "test_async_mongo_writer"


def _cleanup():
    MongoClient(MONGO_HOST, 27017).drop_database(DB_NAME)


def _get_db():
    return MongoClient(MONGO_HOST, 27017)[DB_NAME]


@pytest.fixture(autouse=True)
def cleanup():
    _cleanup()
    yield
    _cleanup()


def _check_mongo():
    try:
        MongoClient(MONGO_HOST, 27017, serverSelectionTimeoutMS=2000).list_database_names()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _check_mongo(),
    reason=f"MongoDB not available at {MONGO_HOST}:27017",
)


def test_write_and_flush():
    """Normal write: documents inserted and verifiable in MongoDB."""
    w = AsyncMongoWriter(MONGO_URI, DB_NAME)

    uid = str(uuid.uuid4())
    w("start", {"uid": uid, "time": time.time(), "plan_name": "test"})
    w("stop", {"uid": str(uuid.uuid4()), "time": time.time(),
               "run_start": uid, "exit_status": "success"})
    w.flush()

    db = _get_db()
    assert db.run_start.count_documents({}) == 1
    assert db.run_stop.count_documents({}) == 1

    doc = db.run_start.find_one({"uid": uid})
    assert doc is not None
    assert doc["plan_name"] == "test"

    w.close()


def test_flush_after_close():
    """flush() after close() returns immediately, no hang."""
    w = AsyncMongoWriter(MONGO_URI, DB_NAME)
    uid = str(uuid.uuid4())
    w("start", {"uid": uid, "time": time.time()})
    w.close()
    w.flush()  # must not hang


def test_double_close():
    """close() twice is safe."""
    w = AsyncMongoWriter(MONGO_URI, DB_NAME)
    w.close()
    w.close()


def test_call_after_close():
    """__call__ after close should not crash (silently drops)."""
    w = AsyncMongoWriter(MONGO_URI, DB_NAME)
    w.close()
    # tx is None, so __call__ silently skips
    w("start", {"uid": str(uuid.uuid4()), "time": time.time()})


def test_error_propagation_bad_uri():
    """flush() raises when insert fails (bad MongoDB URI)."""
    w = AsyncMongoWriter("mongodb://192.0.2.1:27017", "fake_db")

    uid = str(uuid.uuid4())
    w("start", {"uid": uid, "time": time.time()})

    with pytest.raises(RuntimeError, match="failed"):
        w.flush()

    w.close()


def test_error_recovery():
    """After flush reports an error, next flush is clean if no new errors."""
    # First: fail with bad URI
    w_bad = AsyncMongoWriter("mongodb://192.0.2.1:27017", "fake_db")
    w_bad("start", {"uid": str(uuid.uuid4()), "time": time.time()})

    with pytest.raises(RuntimeError):
        w_bad.flush()

    # The error list was drained — second flush should be clean
    # (no new documents were sent, so no new errors)
    w_bad.flush()  # should not raise

    w_bad.close()

    # Second: succeed with good URI
    w_good = AsyncMongoWriter(MONGO_URI, DB_NAME)
    uid = str(uuid.uuid4())
    w_good("start", {"uid": uid, "time": time.time()})
    w_good.flush()  # should not raise

    db = _get_db()
    assert db.run_start.count_documents({}) == 1

    w_good.close()


def test_batch_insert():
    """Multiple documents batched and inserted correctly."""
    w = AsyncMongoWriter(MONGO_URI, DB_NAME, batch_size=5)

    for i in range(12):
        w("event", {
            "uid": str(uuid.uuid4()),
            "time": time.time(),
            "seq_num": i,
            "data": {"value": i * 1.5},
            "timestamps": {"value": time.time()},
        })

    w.flush()

    db = _get_db()
    assert db.event.count_documents({}) == 12

    w.close()
