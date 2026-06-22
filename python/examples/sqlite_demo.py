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

"""Query SQLite data with Python DataFusion.

Install dependencies:

    python -m pip install datafusion pyarrow

Run:

    python python/examples/sqlite_demo.py
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from pathlib import Path

import pyarrow as pa
from datafusion import SessionContext


def default_database_path() -> Path:
    return Path(tempfile.gettempdir()) / "datafusion_python_sample.sqlite"


def prepare_database(database_path: Path) -> None:
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS datafusion_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                total REAL NOT NULL
            );

            DELETE FROM datafusion_orders;

            INSERT INTO datafusion_orders (id, customer, total) VALUES
                (1, 'alice', 19.99),
                (2, 'bob', 7.50),
                (3, 'alice', 100.00),
                (4, 'carol', 42.25);
            """
        )


def read_orders_batch(database_path: Path) -> pa.RecordBatch:
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT id, customer, total
            FROM datafusion_orders
            ORDER BY id
            """
        ).fetchall()

    return pa.RecordBatch.from_pydict(
        {
            "id": [row["id"] for row in rows],
            "customer": [row["customer"] for row in rows],
            "total": [row["total"] for row in rows],
        }
    )


def main() -> None:
    total_start = time.perf_counter()
    database_path = Path(
        os.environ.get("DATAFUSION_SQLITE_DATABASE", default_database_path())
    )

    prepare_start = time.perf_counter()
    prepare_database(database_path)
    prepare_elapsed = time.perf_counter() - prepare_start

    ctx = SessionContext()

    load_start = time.perf_counter()
    orders = read_orders_batch(database_path)
    ctx.register_record_batches("datafusion_orders", [[orders]])
    load_elapsed = time.perf_counter() - load_start

    print("Customer spend from SQLite data loaded into DataFusion:")
    query_start = time.perf_counter()
    ctx.sql(
        """
        SELECT customer, SUM(total) AS spend
        FROM datafusion_orders
        GROUP BY customer
        ORDER BY customer
        """
    ).show()
    query_elapsed = time.perf_counter() - query_start
    total_elapsed = time.perf_counter() - total_start

    print()
    print("Timing:")
    print(f"  SQLite prepare: {prepare_elapsed * 1000:.2f} ms")
    print(f"  Load/register:   {load_elapsed * 1000:.2f} ms")
    print(f"  Query + show:    {query_elapsed * 1000:.2f} ms")
    print(f"  Total:           {total_elapsed * 1000:.2f} ms")


if __name__ == "__main__":
    main()
