#!/usr/bin/env python3
"""Create a localstack SQS queue and send a test message."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_archiver.localstack_sqs_demo import main


if __name__ == "__main__":
    main()
