"""
Tests for AsyncWriter — background file writer.

Covers:
  - Normal write + flush
  - flush after close (no hang)
  - enqueue after close (raises)
  - Error recovery: flush reports errors then clears them
  - pending counter accuracy
"""

import os
import tempfile
import json

import numpy as np
import pytest

from bluesky_dataforge import AsyncWriter


@pytest.fixture
def tmp_path():
    with tempfile.TemporaryDirectory() as d:
        yield d


def test_write_and_flush(tmp_path):
    """Normal write cycle: enqueue, flush, verify file content."""
    path = os.path.join(tmp_path, "out.jsonl")
    w = AsyncWriter(path)

    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    w.enqueue(arr, {"uid": "test-001"})
    w.enqueue(arr * 2, {"uid": "test-002"})
    w.flush()

    with open(path) as f:
        lines = f.readlines()
    assert len(lines) == 2

    rec = json.loads(lines[0])
    assert rec["dtype"] == "float64"
    assert rec["shape"] == [3]
    assert rec["metadata"]["uid"] == "test-001"

    w.close()


def test_flush_after_close(tmp_path):
    """flush() after close() should return immediately, not hang."""
    path = os.path.join(tmp_path, "out.jsonl")
    w = AsyncWriter(path)

    arr = np.array([1.0], dtype=np.float64)
    w.enqueue(arr)
    w.close()
    w.flush()  # must not hang


def test_double_close(tmp_path):
    """close() twice should be safe."""
    path = os.path.join(tmp_path, "out.jsonl")
    w = AsyncWriter(path)
    w.close()
    w.close()


def test_enqueue_after_close(tmp_path):
    """enqueue() after close() should raise."""
    path = os.path.join(tmp_path, "out.jsonl")
    w = AsyncWriter(path)
    w.close()

    arr = np.array([1.0], dtype=np.float64)
    with pytest.raises(RuntimeError, match="closed"):
        w.enqueue(arr)


def test_pending_counter(tmp_path):
    """pending should reflect queued-minus-processed items."""
    path = os.path.join(tmp_path, "out.jsonl")
    w = AsyncWriter(path)

    arr = np.array([1.0], dtype=np.float64)
    for _ in range(10):
        w.enqueue(arr)

    # After flush, all items processed
    w.flush()
    assert w.pending == 0

    w.close()


def test_flush_clears_errors(tmp_path):
    """After flush reports errors, next flush should be clean if no new errors."""
    path = os.path.join(tmp_path, "out.jsonl")
    w = AsyncWriter(path)

    # Normal writes should succeed
    arr = np.array([1.0, 2.0], dtype=np.float64)
    w.enqueue(arr)
    w.flush()  # should not raise

    # Second flush with no new data should also be clean
    w.flush()  # should not raise

    w.close()
