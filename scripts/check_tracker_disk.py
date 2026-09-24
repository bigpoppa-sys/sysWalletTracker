#!/usr/bin/env python3
"""Refuse a node start before disk pressure can affect colocated services."""

import shutil
import sys

free = shutil.disk_usage("/srv/syswallettracker").free
if free < 20 * 1024**3:
    print("Syscoin paused: less than 20 GiB free disk space", file=sys.stderr)
    raise SystemExit(1)
