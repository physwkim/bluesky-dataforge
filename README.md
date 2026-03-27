# bluesky-dataforge

Rust-accelerated data path for [bluesky](https://github.com/bluesky/bluesky).

Offloads document serialization, file I/O, and MongoDB inserts to Rust background threads so Python can proceed to the next scan immediately.

## Installation

```bash
pip install bluesky-dataforge
```

Building from source requires a Rust toolchain (1.85+):

```bash
pip install maturin
maturin develop
```

## Components

### ForgeSubscriber — File Writer

Drop-in replacement for `RE.subscribe()` that writes documents to JSONL files with the GIL released.

```python
from bluesky import RunEngine
from bluesky_dataforge import ForgeSubscriber

RE = RunEngine()
sub = ForgeSubscriber("/tmp/run.jsonl")
RE.subscribe(sub)
RE(count([det], num=100))
sub.close()
```

### AsyncWriter — Array Data Writer

Background file writer for large array data (fly scan waveforms, detector images). `enqueue()` copies array data and returns immediately; Rust writes to disk in the background.

```python
from bluesky_dataforge import AsyncWriter
import numpy as np

writer = AsyncWriter("/data/scan.bin", format="raw")

for frame in detector_frames:
    array = read_pv_array(det_pv)        # PV read (blocking)
    writer.enqueue(array, {"frame": i})   # memcpy only, returns immediately
    reset_pv(det_pv)                      # Python continues immediately

writer.flush()  # wait for all background writes to complete
writer.close()
```

### AsyncMongoWriter — MongoDB Writer

Background MongoDB writer using the Rust `mongodb` driver. Documents are batched and bulk-inserted for efficiency.

```python
from bluesky_dataforge import AsyncMongoWriter

writer = AsyncMongoWriter(
    "mongodb://localhost:27017",
    "metadatastore",
    batch_size=100,
)

# Replace db.insert with background writer
RE.subscribe(writer)
RE(fly_plan(...))
writer.flush()  # wait for all pending inserts
writer.close()
```

## Fly Scan Example

Combine with [ophyd-epicsrs](https://github.com/physwkim/ophyd-epicsrs) `bulk_caget` for maximum throughput:

```python
from ophyd_epicsrs import EpicsRsContext
from bluesky_dataforge import AsyncMongoWriter

ctx = EpicsRsContext()
writer = AsyncMongoWriter("mongodb://localhost:27017", "metadatastore")
RE.subscribe(writer)

class FastXRFFlyer:
    def collect_pages(self):
        # 1. Read all PVs in parallel (~1ms for 50+ PVs)
        pvs = [enc_pv, i0_pv] + roi_pvs + deadtime_pvs
        raw = ctx.bulk_caget(pvs)

        # 2. Deadtime correction (numpy)
        enc = np.array(raw[enc_pv])[:n]
        i0 = np.array(raw[i0_pv])[:n]
        ...

        # 3. Yield single EventPage
        yield {
            "data": {"ENC": enc.tolist(), "I0": i0.tolist(), ...},
            "timestamps": {k: ts for k in keys},
            "time": ts,
            "seq_num": list(range(1, n + 1)),
        }
        # → AsyncMongoWriter: background BSON + insert_many
        # → Python free for next scan
```

**Performance comparison:**

| Step | Before (sequential) | After (parallel + EventPage) |
|------|-------------------|---------------------------|
| PV read | 50 PVs × 30ms = 1500ms | bulk_caget = ~1ms |
| Data save | N rows × 5ms = 500ms | 1 EventPage → background |
| **Total** | **~2000ms** | **~2ms** (Python free) |

## Architecture

```
Python (bluesky RunEngine)
  │
  ├── ForgeSubscriber.__call__()
  │     → py_to_json (GIL held) → py.allow_threads → Rust file I/O
  │
  ├── AsyncWriter.enqueue()
  │     → tobytes + memcpy → mpsc queue → Rust background thread → file write
  │
  └── AsyncMongoWriter.__call__()
        → py_to_json → mpsc queue → Rust background thread
                                      → json_to_bson → batch accumulate
                                      → mongodb::insert_many
```

## Requirements

- Python >= 3.8
- bluesky >= 1.12
- numpy >= 1.24
- MongoDB (for AsyncMongoWriter)

## Related

- [ophyd-epicsrs](https://github.com/physwkim/ophyd-epicsrs) — Rust EPICS CA backend for ophyd (bulk_caget)
- [epics-rs](https://github.com/epics-rs/epics-rs) — Pure Rust EPICS implementation

## License

BSD 3-Clause
