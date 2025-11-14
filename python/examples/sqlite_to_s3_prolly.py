#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
SQLite to S3-backed ProllyTree Import Script

Reads tables from a SQLite database and imports them into an S3-backed ProllyTree.
Data is encoded as: /<table>/<primary_key> => [col1, col2, col3, ...]

The script persists all data to S3 and outputs the root hash, which can be used
to restore the tree in a later session using TreeConfig(root_hash=...).

Usage:
    python sqlite_to_s3_prolly.py <sqlite_db_path> <s3_bucket> [--endpoint-url URL] [--batch-size N]

Example:
    # With LocalStack
    python sqlite_to_s3_prolly.py mydata.db prollytree-test --endpoint-url http://127.0.0.1:4566 --batch-size 1000

    # With real S3
    python sqlite_to_s3_prolly.py mydata.db my-bucket --batch-size 1000

After import, use the printed instructions to restore the tree in another session.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from prollytree import ProllyTree, S3Config


def get_table_info(cursor: sqlite3.Cursor, table_name: str) -> Dict[str, Any]:
    """Get column information for a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    # Find primary key columns
    pk_cols = [col[1] for col in columns if col[5] > 0]  # col[5] is pk flag

    # If no explicit PK, use rowid
    if not pk_cols:
        pk_cols = ["rowid"]

    all_cols = [col[1] for col in columns]

    return {
        "columns": all_cols,
        "primary_keys": pk_cols,
        "column_types": {col[1]: col[2] for col in columns}
    }


def get_all_tables(cursor: sqlite3.Cursor) -> List[str]:
    """Get all user tables from the database."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [row[0] for row in cursor.fetchall()]


def encode_value(value: Any) -> str:
    """Encode a Python value to a JSON string."""
    return json.dumps(value, default=str)


def build_primary_key(row_dict: Dict[str, Any], pk_cols: List[str]) -> str:
    """Build a primary key string from row data."""
    if len(pk_cols) == 1:
        return str(row_dict[pk_cols[0]])
    else:
        # Composite key: join with ':'
        return ":".join(str(row_dict[col]) for col in pk_cols)


def import_table(
    cursor: sqlite3.Cursor,
    tree: ProllyTree,
    table_name: str,
    batch_size: int = 1000,
    verbose: bool = True
) -> int:
    """Import a single table into the ProllyTree."""

    # Get table metadata
    info = get_table_info(cursor, table_name)
    columns = info["columns"]
    pk_cols = info["primary_keys"]

    if verbose:
        print(f"\nImporting table: {table_name}")
        print(f"  Columns: {', '.join(columns)}")
        print(f"  Primary key(s): {', '.join(pk_cols)}")

    # Build SELECT query
    if "rowid" in pk_cols and "rowid" not in columns:
        # Need to explicitly select rowid
        select_cols = ["rowid"] + columns
    else:
        select_cols = columns

    query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
    cursor.execute(query)

    # Fetch and insert in batches
    total_rows = 0
    batch = []

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        for row in rows:
            # Convert row to dict
            row_dict = dict(zip(select_cols, row))

            # Build key: /<table>/<primary_key>
            pk_value = build_primary_key(row_dict, pk_cols)
            key = f"/{table_name}/{pk_value}"

            # Build value: JSON array of column values in order
            value_list = [row_dict.get(col) for col in columns]
            value = encode_value(value_list)

            batch.append((key, value))

        # Insert batch
        if batch:
            # Convert to bytes for ProllyTree
            byte_batch = [(k.encode('utf-8'), v.encode('utf-8')) for k, v in batch]
            tree.insert_batch(byte_batch)
            total_rows += len(batch)

            if verbose:
                print(f"  Inserted {total_rows} rows...", end="\r")

            batch = []

    if verbose:
        print(f"  Inserted {total_rows} rows - DONE")

    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description="Import SQLite database into S3-backed ProllyTree"
    )
    parser.add_argument("sqlite_db", help="Path to SQLite database file")
    parser.add_argument("s3_bucket", help="S3 bucket name")
    parser.add_argument(
        "--endpoint-url",
        help="S3 endpoint URL (for LocalStack or S3-compatible services)",
        default=None
    )
    parser.add_argument(
        "--region",
        help="AWS region (default: us-east-1)",
        default="us-east-1"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of rows to insert per batch (default: 1000)",
        default=1000
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to import (default: all tables)",
        default=None
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )

    args = parser.parse_args()

    # Validate SQLite database exists
    if not Path(args.sqlite_db).exists():
        print(f"Error: SQLite database not found: {args.sqlite_db}")
        sys.exit(1)

    # Connect to SQLite
    if not args.quiet:
        print(f"Opening SQLite database: {args.sqlite_db}")

    conn = sqlite3.connect(args.sqlite_db)
    cursor = conn.cursor()

    # Get tables to import
    if args.tables:
        tables = args.tables
    else:
        tables = get_all_tables(cursor)

    if not tables:
        print("No tables found to import")
        sys.exit(0)

    if not args.quiet:
        print(f"Tables to import: {', '.join(tables)}")

    # Create S3-backed ProllyTree
    if not args.quiet:
        print(f"\nInitializing S3-backed ProllyTree")
        print(f"  Bucket: {args.s3_bucket}")
        print(f"  Region: {args.region}")
        if args.endpoint_url:
            print(f"  Endpoint: {args.endpoint_url}")

    s3_config = S3Config(
        bucket=args.s3_bucket,
        prefix="sqlite-import/",
        region=args.region,
        endpoint_url=args.endpoint_url,
        cache_size=1000
    )

    tree = ProllyTree(storage_type="s3", s3_config=s3_config)

    # Import each table
    total_rows = 0
    for table in tables:
        try:
            rows = import_table(
                cursor,
                tree,
                table,
                batch_size=args.batch_size,
                verbose=not args.quiet
            )
            total_rows += rows
        except Exception as e:
            print(f"Error importing table {table}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Close SQLite connection
    conn.close()

    root_hash = tree.get_root_hash()
    root_hash_hex = root_hash.hex()

    if not args.quiet:
        print(f"\n{'='*60}")
        print(f"Import complete!")
        print(f"  Total rows imported: {total_rows}")
        print(f"  Root hash: {root_hash_hex}")
        print(f"")
        print(f"To access this data later, use:")
        print(f"  from prollytree import ProllyTree, S3Config, TreeConfig")
        print(f"  s3_config = S3Config(bucket='{args.s3_bucket}', prefix='sqlite-import/', \\")
        print(f"                       region='{args.region}', endpoint_url={repr(args.endpoint_url)})")
        print(f"  config = TreeConfig(root_hash=bytes.fromhex('{root_hash_hex}'))")
        print(f"  tree = ProllyTree(storage_type='s3', s3_config=s3_config, config=config)")
        print(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())