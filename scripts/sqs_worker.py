#!/usr/bin/env python3
"""Simple SQS worker skeleton.

Usage:
  python scripts/sqs_worker.py --queue-url <url>

Environment overrides:
  AWS_REGION, SQS_ENDPOINT_URL
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_archiver.sqs_worker import main


if __name__ == "__main__":
    raise SystemExit(main())
