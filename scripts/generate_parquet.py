#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Generate a synthetic Parquet file for local DataFusion benchmarks.

The script writes record batches incrementally, so it can create files much
larger than memory. Install dependencies with:

    python -m pip install pyarrow numpy

Example:

    python scripts/generate_parquet.py --rows 10000000 --output benchmark-data/large.parquet
"""

from __future__ import annotations

import argparse
import math
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_ROWS = 10_000_000
DEFAULT_BATCH_SIZE = 250_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a large synthetic Parquet file for benchmark data."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows to generate. Default: {DEFAULT_ROWS:,}.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per generated batch. Default: {DEFAULT_BATCH_SIZE:,}.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark-data/large.parquet"),
        help="Output Parquet path. Default: benchmark-data/large.parquet.",
    )
    parser.add_argument(
        "--compression",
        choices=("none", "snappy", "zstd", "gzip", "brotli", "lz4"),
        default="zstd",
        help="Parquet compression codec. Default: zstd.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic data. Default: 42.",
    )
    return parser.parse_args()


def make_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.int64()),
            ("group_id", pa.int32()),
            ("user_id", pa.int64()),
            ("amount", pa.float64()),
            ("quantity", pa.int32()),
            ("event_date", pa.date32()),
            ("category", pa.string()),
            ("payload", pa.string()),
            ("is_active", pa.bool_()),
        ]
    )


def make_batch(start: int, count: int, rng: np.random.Generator) -> pa.RecordBatch:
    ids = np.arange(start, start + count, dtype=np.int64)
    group_ids = (ids % 1_024).astype(np.int32)
    user_ids = rng.integers(1, 2_000_000, size=count, dtype=np.int64)
    amounts = rng.normal(loc=100.0, scale=35.0, size=count).round(4)
    quantities = rng.integers(1, 50, size=count, dtype=np.int32)
    event_days = (ids % 3_650).astype(np.int32)
    categories = np.array([f"category_{value % 64:02d}" for value in ids], dtype=object)
    payloads = np.array([f"payload-{value:016x}" for value in ids], dtype=object)
    is_active = (ids % 7) != 0

    return pa.record_batch(
        [
            pa.array(ids),
            pa.array(group_ids),
            pa.array(user_ids),
            pa.array(amounts),
            pa.array(quantities),
            pa.array(event_days, type=pa.date32()),
            pa.array(categories),
            pa.array(payloads),
            pa.array(is_active),
        ],
        schema=make_schema(),
    )


def main() -> None:
    args = parse_args()
    if args.rows <= 0:
        raise ValueError("--rows must be positive")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")

    output: Path = args.output
    output.parent.mkdir(parents=True, exist_ok=True)

    compression = None if args.compression == "none" else args.compression
    rng = np.random.default_rng(args.seed)
    schema = make_schema()

    started = time.perf_counter()
    total_batches = math.ceil(args.rows / args.batch_size)

    with pq.ParquetWriter(
        output,
        schema,
        compression=compression,
        use_dictionary=["category", "payload"],
        write_statistics=True,
    ) as writer:
        for batch_index, start in enumerate(range(0, args.rows, args.batch_size), start=1):
            count = min(args.batch_size, args.rows - start)
            batch = make_batch(start, count, rng)
            writer.write_batch(batch)

            elapsed = time.perf_counter() - started
            print(
                f"[{batch_index:>4}/{total_batches}] "
                f"rows={start + count:,}/{args.rows:,} elapsed={elapsed:.1f}s",
                flush=True,
            )

    elapsed = time.perf_counter() - started
    size_mb = output.stat().st_size / 1024 / 1024
    print(f"wrote {args.rows:,} rows to {output} ({size_mb:.1f} MiB) in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
