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

"""Run join queries over sample API key CSV files."""

from __future__ import annotations

from pathlib import Path

from datafusion import SessionContext


DATA_DIR = Path(__file__).parent / "data"


def main() -> None:
    ctx = SessionContext()
    ctx.register_csv("api_keys", DATA_DIR / "api_keys.csv")
    ctx.register_csv("api_usage", DATA_DIR / "api_usage.csv")
    ctx.register_csv("api_tags", DATA_DIR / "api_tags.csv")

    print("API usage joined with key metadata:")
    ctx.sql(
        """
        SELECT
            k.key,
            k.http_method,
            k.entity,
            k.scope,
            u.request_count,
            u.error_count,
            u.avg_latency_ms
        FROM api_keys AS k
        JOIN api_usage AS u ON k.key = u.key
        ORDER BY u.request_count DESC
        """
    ).show()

    print("Request count by scope:")
    ctx.sql(
        """
        SELECT
            k.scope,
            SUM(u.request_count) AS total_requests,
            SUM(u.error_count) AS total_errors,
            AVG(u.avg_latency_ms) AS avg_latency_ms
        FROM api_keys AS k
        JOIN api_usage AS u ON k.key = u.key
        GROUP BY k.scope
        ORDER BY total_requests DESC
        """
    ).show()

    print("Asset APIs with tags:")
    ctx.sql(
        """
        SELECT
            k.key,
            k.http_method,
            k.operation,
            t.tag,
            u.request_count
        FROM api_keys AS k
        JOIN api_usage AS u ON k.key = u.key
        JOIN api_tags AS t ON k.key = t.key
        WHERE k.entity = 'Asset'
        ORDER BY k.key, t.tag
        """
    ).show()


if __name__ == "__main__":
    main()
