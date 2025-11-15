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
CLI for ProllyTree database operations.

Provides commands for importing SQLite databases and dumping data.
"""

import argparse
import sqlite3
import json
import time
from typing import Optional, List

from db import DB
from store import CachedFSStore, create_store_from_spec
from diff import Differ, Added, Deleted, Modified


def import_sqlite_table(db: DB, sqlite_conn: sqlite3.Connection, table_name: str,
                        batch_size: int = 1000, verbose_batches: bool = False) -> int:
    """
    Import a single SQLite table into the database.

    Args:
        db: DB instance
        sqlite_conn: SQLite connection
        table_name: Name of table to import
        batch_size: Batch size for inserts
        verbose_batches: Show detailed batch statistics

    Returns:
        Number of rows imported
    """
    cursor = sqlite_conn.cursor()

    # Get primary key columns
    cursor.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()

    pk_columns = []
    for row in rows:
        if row[5]:  # pk column is at index 5
            pk_columns.append((row[5], row[1]))  # (pk_position, column_name)

    if pk_columns:
        pk_columns.sort(key=lambda x: x[0])
        primary_key = [col[1] for col in pk_columns]
    else:
        primary_key = ["rowid"]

    # Get column names and types
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    columns = [row[1] for row in table_info]
    column_types = [row[2] for row in table_info]

    # Display table info
    if len(primary_key) == 1:
        print(f"Primary key: {primary_key[0]}")
    else:
        print(f"Primary key (compound): {', '.join(primary_key)}")

    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    print(f"Total rows: {row_count:,}")

    if row_count == 0:
        print("Skipping empty table")
        return 0

    # Create table in DB
    print(f"Stored schema: {len(columns)} columns")
    db.create_table(table_name, columns, column_types, primary_key)

    # Prepare row iterator
    if primary_key == ["rowid"]:
        cursor.execute(f"SELECT rowid, * FROM {table_name}")
    else:
        cursor.execute(f"SELECT * FROM {table_name}")

    def row_generator():
        """Generator that yields rows from cursor."""
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                if primary_key == ["rowid"]:
                    # Skip rowid in the values, it's handled by insert_rows
                    yield row
                else:
                    yield row

    # Insert rows
    table_start = time.time()
    rows_processed = db.insert_rows(table_name, row_generator(),
                                    batch_size=batch_size,
                                    verbose=verbose_batches)

    table_time = time.time() - table_start
    table_rate = rows_processed / table_time if table_time > 0 else 0

    print(f"\nCompleted {table_name}: {rows_processed:,} rows in {table_time:.2f}s "
          f"({table_rate:,.0f} rows/sec)")

    # Print root hash
    root_hash = db.get_root_hash()
    print(f"  Root hash: {root_hash}")

    # Show cache stats if available
    store = db.get_store()
    if isinstance(store, CachedFSStore):
        creation_stats = store.get_creation_stats()
        cache_stats = store.get_cache_stats()
        print(f"  Cumulative: {creation_stats['total_leaves_created']:,} leaves, "
              f"{creation_stats['total_internals_created']:,} internals created")
        print(f"  Cache: {cache_stats['cache_evictions']:,} evictions, "
              f"{cache_stats['cache_size']:,}/{cache_stats['max_cache_size']:,} entries, "
              f"{cache_stats['hit_rate']} hit rate")

        # Print size distributions
        print()
        store.print_distributions(bucket_count=10)

    return rows_processed


def import_sqlite_database(db_path: str, store_spec: str = ':memory:',
                           pattern: float = 0.0001, seed: int = 42,
                           cache_size: Optional[int] = None,
                           batch_size: int = 1000,
                           tables_filter: Optional[List[str]] = None,
                           verbose_batches: bool = False) -> DB:
    """
    Import a SQLite database into ProllyTree.

    Args:
        db_path: Path to SQLite database
        store_spec: Store specification
        pattern: ProllyTree split pattern
        seed: Random seed
        cache_size: Cache size for cached stores
        batch_size: Batch size for inserts
        tables_filter: Optional list of table names to import
        verbose_batches: Show detailed batch statistics

    Returns:
        DB instance
    """
    print(f"Opening database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    all_tables = [row[0] for row in cursor.fetchall()]

    # Filter tables if requested
    if tables_filter:
        tables = [t for t in all_tables if t in tables_filter]
        print(f"Found {len(all_tables)} tables, importing {len(tables)}: {', '.join(tables)}")
        skipped = [t for t in tables_filter if t not in all_tables]
        if skipped:
            print(f"Warning: Requested tables not found: {', '.join(skipped)}")
    else:
        tables = all_tables
        print(f"Found {len(tables)} tables: {', '.join(tables)}")

    # Create DB
    print(f"\nInitializing ProllyTree (pattern={pattern}, seed={seed}, store={store_spec})")
    if cache_size:
        print(f"  Cache size: {cache_size}")

    store = create_store_from_spec(store_spec, cache_size=cache_size)
    db = DB(store=store, pattern=pattern, seed=seed)

    # Import each table
    total_rows = 0
    total_start = time.time()

    for table_name in tables:
        print(f"\n{'='*80}")
        print(f"Importing table: {table_name}")
        print(f"{'='*80}")

        rows_imported = import_sqlite_table(db, conn, table_name,
                                            batch_size=batch_size,
                                            verbose_batches=verbose_batches)
        total_rows += rows_imported

    total_time = time.time() - total_start
    total_rate = total_rows / total_time if total_time > 0 else 0

    print(f"\n{'='*80}")
    print(f"IMPORT COMPLETE")
    print(f"{'='*80}")
    print(f"Total rows imported: {total_rows:,}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Overall rate: {total_rate:,.0f} rows/sec")

    # Print final stats
    store = db.get_store()
    print(f"\nTree statistics:")
    print(f"  Store type: {type(store).__name__}")
    print(f"  Total nodes in storage: {store.count_nodes():,}")

    if isinstance(store, CachedFSStore):
        stats = store.get_cache_stats()
        print(f"\nCache statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    conn.close()
    return db


def dump_database(store_spec: str, prefix: str, root_hash: Optional[str] = None,
                  cache_size: Optional[int] = None, reconstruct: bool = True,
                  limit: int = 100):
    """
    Dump keys from the database.

    Args:
        store_spec: Store specification
        prefix: Key prefix to dump
        root_hash: Optional root hash to load
        cache_size: Cache size for cached stores
        reconstruct: Reconstruct row objects
        limit: Maximum number of rows to display
    """
    from tree import ProllyTree

    print(f"Opening store: {store_spec}")
    store = create_store_from_spec(store_spec, cache_size=cache_size)

    # Count nodes
    node_count = store.count_nodes()
    print(f"Store contains {node_count:,} total nodes")

    if node_count == 0:
        print("Store is empty")
        return

    # Create tree
    tree = ProllyTree(pattern=0.0001, seed=42, store=store)

    if root_hash:
        print(f"Loading tree from root hash: {root_hash}")
        tree.root = store.get_node(root_hash)
        if not tree.root:
            print(f"Error: Root hash {root_hash} not found in store")
            return
    else:
        print("Warning: No root hash provided")
        print("For accurate results, provide --root-hash parameter")

    # Create DB wrapper
    db = DB(store=store, pattern=0.0001, seed=42)

    # Check if this is a schema or data dump
    if prefix.startswith('/s/'):
        # Dumping schemas
        print(f"\nSchemas:")
        for key, value in tree.items(prefix):
            print(f"{key} => {value}")
    elif prefix.startswith('/d/'):
        # Dumping data
        # Extract table name
        parts = prefix.split('/')
        if len(parts) >= 3:
            table_name = parts[2]
            row_prefix = "/".join(parts[3:]) if len(parts) > 3 else ""

            print(f"\nData from table: {table_name}")
            count = 0
            for key, row_data in db.read_rows(table_name, prefix=row_prefix,
                                             reconstruct=reconstruct):
                count += 1
                if count <= limit:
                    if reconstruct:
                        print(f"{key} => {json.dumps(row_data, separators=(',', ':'))}")
                    else:
                        print(f"{key} => {row_data}")

            print(f"\nTotal: {count:,} rows found")
            if count > limit:
                print(f"(showing first {limit}, {count - limit:,} more rows omitted)")
        else:
            print("Error: Invalid prefix format for data dump")
    else:
        # Generic dump
        print(f"\nKeys with prefix: {prefix}")
        count = 0
        for key, value in tree.items(prefix):
            count += 1
            if count <= limit:
                print(f"{key} => {value}")

        print(f"\nTotal: {count:,} keys found")
        if count > limit:
            print(f"(showing first {limit}, {count - limit:,} more keys omitted)")


def diff_trees(old_hash: str, new_hash: str,
                store_spec: str = 'cached-file://.prolly',
                cache_size: Optional[int] = None,
                limit: Optional[int] = None,
                prefix: Optional[str] = None):
    """
    Diff two trees by their root hashes.

    Args:
        old_hash: Root hash of old tree
        new_hash: Root hash of new tree
        store_spec: Store specification
        cache_size: Cache size for cached stores
        limit: Maximum number of diff events to display (None for all)
        prefix: Optional key prefix to filter diff results
    """
    print("="*80)
    print("DIFF: Comparing two trees by hash")
    print("="*80)
    print(f"Old hash: {old_hash}")
    print(f"New hash: {new_hash}")
    print(f"Store:    {store_spec}")
    if prefix:
        print(f"Prefix:   {prefix}")

    if old_hash == new_hash:
        print("\nTrees are identical (same root hash)")
        return

    store = create_store_from_spec(store_spec, cache_size=cache_size)

    # Create Differ instance to track statistics
    differ = Differ(store)

    print(f"\nDiff events (old -> new):")
    print("-"*80)

    event_count = 0
    added_count = 0
    deleted_count = 0
    modified_count = 0

    for event in differ.diff(old_hash, new_hash, prefix=prefix):
        event_count += 1

        if limit is None or event_count <= limit:
            if isinstance(event, Added):
                print(f"+ {event.key} = {event.value}")
                added_count += 1
            elif isinstance(event, Deleted):
                print(f"- {event.key} = {event.old_value}")
                deleted_count += 1
            elif isinstance(event, Modified):
                print(f"M {event.key}: {event.old_value} -> {event.new_value}")
                modified_count += 1
        else:
            # Just count without printing
            if isinstance(event, Added):
                added_count += 1
            elif isinstance(event, Deleted):
                deleted_count += 1
            elif isinstance(event, Modified):
                modified_count += 1

    print("-"*80)
    print(f"\nDiff Summary:")
    print(f"  Added:    {added_count:,}")
    print(f"  Deleted:  {deleted_count:,}")
    print(f"  Modified: {modified_count:,}")
    print(f"  Total:    {event_count:,}")

    if limit is not None and event_count > limit:
        print(f"\n(showing first {limit}, {event_count - limit:,} more events omitted)")

    # Print diff statistics
    diff_stats = differ.get_stats()
    print(f"\nDiff Algorithm Statistics:")
    print(f"  Subtrees skipped (identical hashes): {diff_stats.subtrees_skipped:,}")
    print(f"  Nodes compared:                      {diff_stats.nodes_compared:,}")

    # Show cache stats if using cached store
    if isinstance(store, CachedFSStore):
        print("\n" + "="*80)
        print("CACHE STATISTICS")
        print("="*80)
        stats = store.get_cache_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='ProllyTree Database CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Import SQLite subcommand
    import_parser = subparsers.add_parser('import-sqlite', help='Import SQLite database into ProllyTree',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Import database
  python cli.py import-sqlite database.sqlite --store file:///tmp/data

  # Import specific tables
  python cli.py import-sqlite database.sqlite --tables buses generators --store file:///tmp/data

  # Import with caching
  python cli.py import-sqlite database.sqlite --store cached-file:///tmp/data --cache-size 1000
        ''')

    import_parser.add_argument('database', help='Path to SQLite database file')
    import_parser.add_argument('--store', default='cached-file://.prolly',
                        help='Store spec (default: cached-file://.prolly)')
    import_parser.add_argument('--pattern', type=float, default=0.0001,
                        help='Split pattern (default: 0.0001)')
    import_parser.add_argument('--seed', type=int, default=42,
                        help='Random seed (default: 42)')
    import_parser.add_argument('--cache-size', type=int, default=None,
                        help='Cache size for cached stores')
    import_parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for inserts (default: 1000)')
    import_parser.add_argument('--tables', nargs='+', default=None,
                        help='Specific table names to import')
    import_parser.add_argument('--verbose-batches', action='store_true',
                        help='Show detailed batch statistics')

    # Dump subcommand
    dump_parser = subparsers.add_parser('dump', help='Dump data from ProllyTree',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Dump schemas
  python cli.py dump /s/ --store file:///tmp/data --root-hash abc123

  # Dump table data
  python cli.py dump /d/buses --store file:///tmp/data --root-hash abc123

  # Dump without reconstruction (raw arrays)
  python cli.py dump /d/buses --store file:///tmp/data --root-hash abc123 --no-reconstruct
        ''')

    dump_parser.add_argument('prefix', help='Key prefix to dump')
    dump_parser.add_argument('--store', default='cached-file://.prolly',
                        help='Store spec (default: cached-file://.prolly)')
    dump_parser.add_argument('--root-hash', type=str, default=None,
                        help='Root hash of tree to dump')
    dump_parser.add_argument('--cache-size', type=int, default=None,
                        help='Cache size for cached stores')
    dump_parser.add_argument('--no-reconstruct', action='store_true',
                        help='Show raw arrays instead of reconstructed objects')
    dump_parser.add_argument('--limit', type=int, default=100,
                        help='Maximum rows to display (default: 100)')

    # Diff subcommand
    diff_parser = subparsers.add_parser('diff', help='Diff two trees by root hash',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Diff two trees
  python cli.py diff a16b213fc2e7d598 8b2d2b8e2c75c085

  # Diff with custom store
  python cli.py diff old_hash new_hash --store cached-file:///tmp/data

  # Limit output
  python cli.py diff old_hash new_hash --limit 100

  # Filter by key prefix
  python cli.py diff old_hash new_hash --prefix /d/table_name
        ''')

    diff_parser.add_argument('old_hash', help='Root hash of old tree')
    diff_parser.add_argument('new_hash', help='Root hash of new tree')
    diff_parser.add_argument('--store', default='cached-file://.prolly',
                        help='Store spec (default: cached-file://.prolly)')
    diff_parser.add_argument('--cache-size', type=int, default=None,
                        help='Cache size for cached stores')
    diff_parser.add_argument('--limit', type=int, default=None,
                        help='Maximum diff events to display (default: all)')
    diff_parser.add_argument('--prefix', type=str, default=None,
                        help='Key prefix to filter diff results')

    args = parser.parse_args()

    if args.command == 'import-sqlite':
        import_sqlite_database(
            db_path=args.database,
            store_spec=args.store,
            pattern=args.pattern,
            seed=args.seed,
            cache_size=args.cache_size,
            batch_size=args.batch_size,
            tables_filter=args.tables,
            verbose_batches=args.verbose_batches
        )
    elif args.command == 'dump':
        dump_database(
            store_spec=args.store,
            prefix=args.prefix,
            root_hash=args.root_hash,
            cache_size=args.cache_size,
            reconstruct=not args.no_reconstruct,
            limit=args.limit
        )
    elif args.command == 'diff':
        diff_trees(
            old_hash=args.old_hash,
            new_hash=args.new_hash,
            store_spec=args.store,
            cache_size=args.cache_size,
            limit=args.limit,
            prefix=args.prefix
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
