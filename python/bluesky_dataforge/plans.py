"""Plan wrappers for bluesky-dataforge.

The standard bluesky plans (count, scan, etc.) work directly with
ophyd devices — these wrappers are optional convenience functions.
"""

from __future__ import annotations


def forge_count(detectors, num=1, delay=None):
    """Wrapper around bluesky.plans.count."""
    from bluesky.plans import count

    return count(detectors, num=num, delay=delay)


def forge_scan(detectors, motor, start, stop, num):
    """Wrapper around bluesky.plans.scan."""
    from bluesky.plans import scan

    return scan(detectors, motor, start, stop, num)
