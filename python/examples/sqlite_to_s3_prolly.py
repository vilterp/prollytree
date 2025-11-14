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
SQLite to S3-backed ProllyTree Tool

Subcommands:
  import  - Import SQLite database into S3-backed ProllyTree
  dump    - Dump ProllyTree contents to stdout

Usage:
    # Import SQLite to ProllyTree (prints root hash after each batch)
    python sqlite_to_s3_prolly.py import <sqlite_db> <s3_bucket> [options]

    # Dump ProllyTree contents
    python sqlite_to_s3_prolly.py dump <s3_bucket> <root_hash> [options]

Examples:
    # Import with LocalStack
    python sqlite_to_s3_prolly.py import mydata.db bucket \
        --endpoint-url http://127.0.0.1:4566 --batch-size 1000

    # Dump from specific root hash
    python sqlite_to_s3_prolly.py dump bucket b87221ff... \
        --endpoint-url http://127.0.0.1:4566
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from prollytree import ProllyTree, S3Config, TreeConfig


def normalize_endpoint_url(endpoint_url: str) -> str:
    """Normalize endpoint URL - replace localhost with 127.0.0.1 for AWS SDK."""
    if endpoint_url and 'localhost' in endpoint_url:
        return endpoint_url.replace('localhost', '127.0.0.1')
    return endpoint_url


def create_s3_config(bucket: str, endpoint_url: str, region: str) -> S3Config:
    """Create S3Config with normalized endpoint."""
    normalized_url = normalize_endpoint_url(endpoint_url) if endpoint_url else None
    return S3Config(
        bucket=bucket,
        prefix="sqlite-import/",
        region=region,
        endpoint_url=normalized_url,
        cache_size=10000
    )


def create_tree_config(size: str = "default") -> TreeConfig:
    """Create TreeConfig with specified node size.

    Args:
        size: One of "default", "small", "medium", "large", "xlarge"
              default: Library defaults (~8 entries, frequent splits)
              small:   ~64KB nodes (pattern 0xFFFF)
              medium:  ~256KB nodes (pattern 0xFFFFF)
              large:   ~1MB nodes (pattern 0xFFFFFF)
              xlarge:  ~4MB nodes (pattern 0xFFFFFFF)
    """
    if size == "default":
        return TreeConfig()  # Use library defaults
    elif size == "small":
        return TreeConfig(
            min_chunk_size=100,
            max_chunk_size=1_000_000,
            pattern=0xFFFF  # 65535 - ~1 in 65K split
        )
    elif size == "medium":
        return TreeConfig(
            min_chunk_size=500,
            max_chunk_size=5_000_000,
            pattern=0xFFFFF  # 1048575 - ~1 in 1M split
        )
    elif size == "large":
        return TreeConfig(
            min_chunk_size=1000,
            max_chunk_size=10_000_000,
            pattern=0xFFFFFF  # 16777215 - ~1 in 16M split
        )
    elif size == "xlarge":
        return TreeConfig(
            min_chunk_size=2000,
            max_chunk_size=20_000_000,
            pattern=0xFFFFFFF  # 268435455 - ~1 in 268M split
        )
    else:
        raise ValueError(f"Unknown size: {size}")


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

            # Insert batch
            tree.insert_batch(byte_batch)
            total_rows += len(batch)

            # Print root hash after each batch
            if verbose:
                root_hash = tree.get_root_hash().hex()
                print(f"  Inserted {total_rows} rows... Root: {root_hash[:16]}...")

            batch = []

    if verbose:
        print(f"  Inserted {total_rows} rows - DONE")

    return total_rows


def cmd_import(args):
    """Import SQLite database into ProllyTree."""

    # Validate SQLite database exists
    if not Path(args.sqlite_db).exists():
        print(f"Error: SQLite database not found: {args.sqlite_db}")
        sys.exit(1)

    # Validate storage-specific requirements
    if args.storage == "s3" and not args.s3_bucket:
        print("Error: S3 bucket required when using --storage=s3")
        sys.exit(1)
    if args.storage == "file" and not args.storage_path:
        print("Error: --storage-path required when using --storage=file")
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

    # Create ProllyTree with specified storage backend
    if not args.quiet:
        print(f"\nInitializing ProllyTree")
        print(f"  Storage: {args.storage}")
        if args.storage == "s3":
            print(f"  Bucket: {args.s3_bucket}")
            print(f"  Region: {args.region}")
            if args.endpoint_url:
                print(f"  Endpoint: {args.endpoint_url}")
                if 'localhost' in args.endpoint_url:
                    print(f"  Note: Will normalize localhost to 127.0.0.1")
        elif args.storage == "file":
            print(f"  Path: {args.storage_path}")
        print(f"  Node size: {args.node_size}")

    tree_config = create_tree_config(args.node_size)

    if args.storage == "s3":
        s3_config = create_s3_config(args.s3_bucket, args.endpoint_url, args.region)
        tree = ProllyTree(storage_type="s3", s3_config=s3_config, config=tree_config)
    elif args.storage == "memory":
        tree = ProllyTree(storage_type="memory", config=tree_config)
    elif args.storage == "file":
        tree = ProllyTree(storage_type="file", path=args.storage_path, config=tree_config)
    else:
        print(f"Error: Unknown storage type: {args.storage}")
        sys.exit(1)

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
        print(f"  Final root hash: {root_hash_hex}")
        print(f"")
        print(f"To dump this data, use:")
        print(f"  python {sys.argv[0]} dump {args.s3_bucket} {root_hash_hex} \\")
        print(f"    --endpoint-url {args.endpoint_url or 'https://s3.amazonaws.com'}")
        print(f"{'='*60}")
    else:
        # In quiet mode, just print the root hash for piping
        print(root_hash_hex)

    return 0


def cmd_dump(args):
    """Dump ProllyTree contents to stdout."""

    if not args.quiet:
        print(f"Loading ProllyTree from S3...", file=sys.stderr)
        print(f"  Bucket: {args.s3_bucket}", file=sys.stderr)
        print(f"  Root hash: {args.root_hash}", file=sys.stderr)
        if args.endpoint_url:
            print(f"  Endpoint: {args.endpoint_url}", file=sys.stderr)

    # Parse root hash
    try:
        root_hash_bytes = bytes.fromhex(args.root_hash)
        if len(root_hash_bytes) != 32:
            print(f"Error: Root hash must be 32 bytes (64 hex characters)", file=sys.stderr)
            sys.exit(1)
    except ValueError as e:
        print(f"Error: Invalid hex string for root hash: {e}", file=sys.stderr)
        sys.exit(1)

    # Create S3-backed ProllyTree with root hash
    s3_config = create_s3_config(args.s3_bucket, args.endpoint_url, args.region)
    tree_config = TreeConfig(root_hash=root_hash_bytes)
    tree = ProllyTree(storage_type="s3", s3_config=s3_config, config=tree_config)

    if not args.quiet:
        print(f"  Tree size: {tree.size()} entries", file=sys.stderr)
        print(f"  Dumping to stdout...", file=sys.stderr)
        print("", file=sys.stderr)

    # Traverse tree and dump all key-value pairs
    def visitor(key: bytes, value: bytes):
        try:
            key_str = key.decode('utf-8')
            value_str = value.decode('utf-8')

            # Output as JSON lines (one object per line)
            output = json.dumps({"key": key_str, "value": json.loads(value_str)})
            print(output)
        except Exception as e:
            if not args.quiet:
                print(f"Warning: Could not decode entry: {e}", file=sys.stderr)

    tree.traverse(visitor)

    if not args.quiet:
        print(f"\nDump complete!", file=sys.stderr)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="SQLite to S3-backed ProllyTree Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest='command', help='Subcommand to run')
    subparsers.required = True

    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import SQLite database to ProllyTree')
    import_parser.add_argument("sqlite_db", help="Path to SQLite database file")
    import_parser.add_argument("s3_bucket", nargs='?', help="S3 bucket name (optional if using --storage=memory)")
    import_parser.add_argument(
        "--storage",
        choices=["s3", "memory", "file"],
        default="s3",
        help="Storage backend to use (default: s3)"
    )
    import_parser.add_argument(
        "--storage-path",
        help="Path for file storage (required if --storage=file)",
        default=None
    )
    import_parser.add_argument(
        "--endpoint-url",
        help="S3 endpoint URL (for LocalStack or S3-compatible services)",
        default=None
    )
    import_parser.add_argument(
        "--region",
        help="AWS region (default: us-east-1)",
        default="us-east-1"
    )
    import_parser.add_argument(
        "--node-size",
        choices=["default", "small", "medium", "large", "xlarge"],
        default="large",
        help="Node size configuration: default (library defaults), small (~64KB), medium (~256KB), large (~1MB), xlarge (~4MB)"
    )
    import_parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of rows to insert per batch (default: 1000)",
        default=1000
    )
    import_parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to import (default: all tables)",
        default=None
    )
    import_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output (only print final root hash)"
    )
    import_parser.set_defaults(func=cmd_import)

    # Dump subcommand
    dump_parser = subparsers.add_parser('dump', help='Dump ProllyTree contents to stdout')
    dump_parser.add_argument("s3_bucket", help="S3 bucket name")
    dump_parser.add_argument("root_hash", help="Root hash (64 hex characters)")
    dump_parser.add_argument(
        "--endpoint-url",
        help="S3 endpoint URL (for LocalStack or S3-compatible services)",
        default=None
    )
    dump_parser.add_argument(
        "--region",
        help="AWS region (default: us-east-1)",
        default="us-east-1"
    )
    dump_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress informational output (stderr)"
    )
    dump_parser.set_defaults(func=cmd_dump)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
