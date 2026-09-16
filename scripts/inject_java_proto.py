#!/usr/bin/env python3
"""Inject java_package options into copied v1alpha1 protos."""

from pathlib import Path
import sys

OUTERS = {
    "health.proto": "HealthProto",
    "membership.proto": "MembershipProto",
    "watch.proto": "WatchProto",
    "events.proto": "EventsProto",
    "locks.proto": "LocksProto",
    "leases.proto": "LeasesProto",
}

NEEDLE = 'option go_package = "github.com/clusdr/clusdr/api/clusdr/v1alpha1;clusdrv1alpha1";'


def main() -> None:
    dst = Path(sys.argv[1] if len(sys.argv) > 1 else "proto/clusdr/v1alpha1")
    for name, outer in OUTERS.items():
        p = dst / name
        text = p.read_text()
        extra = (
            NEEDLE
            + "\noption java_multiple_files = true;\n"
            + 'option java_package = "io.clusdr.v1alpha1";\n'
            + f'option java_outer_classname = "{outer}";'
        )
        if "option java_package" in text:
            continue
        if NEEDLE not in text:
            raise SystemExit(f"missing go_package in {p}")
        p.write_text(text.replace(NEEDLE, extra, 1))


if __name__ == "__main__":
    main()
