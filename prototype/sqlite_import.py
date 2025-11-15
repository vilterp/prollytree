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
Import SQLite database into ProllyTree.

Stores key-value pairs as: /<table>/<primary_key> => <row_json>
"""

import sqlite3
import json
import time
import sys
import argparse
import os
import glob
from tree import ProllyTree
from store import create_store_from_spec, CachedFSStore

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def get_primary_key(cursor, table_name):
    """Get the primary key column(s) for a table.

    Returns:
        list: List of primary key column names (may be compound key)
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()

    # Collect all columns that are part of the primary key
    pk_columns = []
    for row in rows:
        if row[5]:  # pk column is at index 5, value indicates position in compound key
            pk_columns.append((row[5], row[1]))  # (pk_position, column_name)

    if pk_columns:
        # Sort by pk position and return column names
        pk_columns.sort(key=lambda x: x[0])
        return [col[1] for col in pk_columns]

    # If no primary key, use rowid
    return ["rowid"]

def import_sqlite(db_path, pattern=0.0001, seed=42, batch_size=1000, store=None, store_spec=':memory:', cache_size=None, verbose_batches=False, tables_filter=None):
    """
    Import all tables from SQLite into ProllyTree.

    Args:
        db_path: Path to SQLite database
        pattern: ProllyTree split pattern (default 0.0001)
        seed: Random seed for rolling hash
        batch_size: Number of rows to insert per batch
        store: Existing store instance to use (optional, for sharing across multiple databases)
        store_spec: Store specification (:memory:, file://path, s3://bucket) - only used if store is None
        cache_size: Cache size for cached stores
        verbose_batches: Show detailed statistics for every batch
        tables_filter: List of table names to import (None = import all)
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

    # Initialize store if not provided
    if store is None:
        print(f"\nInitializing ProllyTree (pattern={pattern}, seed={seed}, store={store_spec})")
        if cache_size:
            print(f"  Cache size: {cache_size}")
        store = create_store_from_spec(store_spec, cache_size=cache_size)
    else:
        print(f"\nInitializing ProllyTree (pattern={pattern}, seed={seed}, using shared store)")

    tree = ProllyTree(pattern=pattern, seed=seed, store=store)

    total_rows = 0
    total_start = time.time()

    for table_name in tables:
        print(f"\n{'='*80}")
        print(f"Importing table: {table_name}")
        print(f"{'='*80}")

        # Get primary key (may be compound)
        pk_columns = get_primary_key(cursor, table_name)
        if len(pk_columns) == 1:
            print(f"Primary key: {pk_columns[0]}")
        else:
            print(f"Primary key (compound): {', '.join(pk_columns)}")

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Total rows: {row_count:,}")

        if row_count == 0:
            print("Skipping empty table")
            continue

        # Get column names and types
        cursor.execute(f"PRAGMA table_info({table_name})")
        table_info = cursor.fetchall()
        columns = [row[1] for row in table_info]
        column_types = [row[2] for row in table_info]

        # Store schema as first mutation
        schema = {
            'columns': columns,
            'types': column_types,
            'primary_key': pk_columns
        }
        schema_key = f"/s/{table_name}"
        schema_value = json.dumps(schema, separators=(',', ':'))

        # Insert schema first
        tree.insert_batch([(schema_key, schema_value)], verbose=False)
        print(f"Stored schema: {len(columns)} columns")

        # Process in batches
        table_start = time.time()
        rows_processed = 0

        # Select with rowid explicitly if needed
        if pk_columns == ["rowid"]:
            cursor.execute(f"SELECT rowid, * FROM {table_name}")
        else:
            cursor.execute(f"SELECT * FROM {table_name}")

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            batch_start = time.time()

            # Prepare mutations for this batch
            mutations = []
            for row in rows:
                # Handle rowid case
                if pk_columns == ["rowid"]:
                    pk_value = str(row[0])
                    # Get values from remaining columns
                    row_values = list(row[1:])
                else:
                    # Build compound primary key from row
                    row_dict = dict(zip(columns, row))
                    pk_parts = [str(row_dict[col]) for col in pk_columns]
                    pk_value = "/".join(pk_parts)
                    # Get all values
                    row_values = list(row)

                # Create key-value pair (data stored as array)
                key = f"/d/{table_name}/{pk_value}"
                value = json.dumps(row_values, separators=(',', ':'))

                mutations.append((key, value))

            # Sort mutations by key (required for prolly tree)
            mutations.sort(key=lambda x: x[0])

            # Insert batch (show verbose stats based on flag or every 10th batch)
            show_verbose = verbose_batches or ((rows_processed // batch_size) % 10 == 0)
            tree.insert_batch(mutations, verbose=show_verbose)

            rows_processed += len(rows)
            batch_time = time.time() - batch_start
            batch_rate = len(rows) / batch_time if batch_time > 0 else 0

            elapsed = time.time() - table_start
            overall_rate = rows_processed / elapsed if elapsed > 0 else 0

            print(f"  Progress: {rows_processed:,}/{row_count:,} rows "
                  f"({rows_processed*100//row_count}%) - "
                  f"Batch: {batch_rate:,.0f} rows/sec - "
                  f"Overall: {overall_rate:,.0f} rows/sec")

        table_time = time.time() - table_start
        table_rate = rows_processed / table_time if table_time > 0 else 0

        print(f"\nCompleted {table_name}: {rows_processed:,} rows in {table_time:.2f}s "
              f"({table_rate:,.0f} rows/sec)")

        # Print root hash for this table
        root_hash = tree._hash_node(tree.root)
        print(f"  Root hash: {root_hash}")

        # Show cumulative node creation stats and size distributions
        if isinstance(tree.store, CachedFSStore):
            creation_stats = tree.store.get_creation_stats()
            cache_stats = tree.store.get_cache_stats()
            print(f"  Cumulative: {creation_stats['total_leaves_created']:,} leaves, "
                  f"{creation_stats['total_internals_created']:,} internals created")
            print(f"  Cache: {cache_stats['cache_evictions']:,} evictions, "
                  f"{cache_stats['cache_size']:,}/{cache_stats['max_cache_size']:,} entries, "
                  f"{cache_stats['hit_rate']} hit rate")

            # Print size distributions
            print()
            tree.store.print_distributions(bucket_count=10)

        total_rows += rows_processed

    total_time = time.time() - total_start
    total_rate = total_rows / total_time if total_time > 0 else 0

    print(f"\n{'='*80}")
    print(f"IMPORT COMPLETE")
    print(f"{'='*80}")
    print(f"Total rows imported: {total_rows:,}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Overall rate: {total_rate:,.0f} rows/sec")
    print(f"\nTree statistics:")
    print(f"  Store type: {type(tree.store).__name__}")
    print(f"  Total nodes in storage: {tree.store.count_nodes():,}")

    # Show cache stats if using CachedFSStore
    if isinstance(tree.store, CachedFSStore):
        stats = tree.store.get_cache_stats()
        print(f"\nCache statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    # Verify we can read some data
    print(f"\nVerifying data...")
    result = tree.verify()
    print(f"  Total key-value pairs in tree: {len(result):,}")

    if len(result) > 0:
        print(f"  First key: {result[0][0]}")
        print(f"  Last key: {result[-1][0]}")

    conn.close()
    return tree


def import_directory(dir_path, pattern=0.0001, seed=42, batch_size=1000, store_spec=':memory:', cache_size=None, verbose_batches=False, tables_filter=None):
    """
    Import all SQLite databases from a directory into separate ProllyTrees sharing the same store.

    Args:
        dir_path: Path to directory containing SQLite database files
        pattern: ProllyTree split pattern (default 0.0001)
        seed: Random seed for rolling hash
        batch_size: Number of rows to insert per batch
        store_spec: Store specification (:memory:, file://path, s3://bucket)
        cache_size: Cache size for cached stores
        verbose_batches: Show detailed statistics for every batch

    Returns:
        dict: Mapping of db_name -> (tree, root_hash)
    """
    # Find all .sqlite files in directory
    db_files = glob.glob(os.path.join(dir_path, '*.sqlite')) + \
               glob.glob(os.path.join(dir_path, '*.db'))

    if not db_files:
        print(f"No SQLite files found in {dir_path}")
        return {}

    print(f"Found {len(db_files)} SQLite databases in {dir_path}")
    for db_file in db_files:
        print(f"  - {os.path.basename(db_file)}")

    # Create shared store
    print(f"\nInitializing shared store: {store_spec}")
    if cache_size:
        print(f"  Cache size: {cache_size}")
    store = create_store_from_spec(store_spec, cache_size=cache_size)

    results = {}
    total_start = time.time()

    for db_path in sorted(db_files):
        db_name = os.path.basename(db_path)
        print(f"\n{'='*80}")
        print(f"Processing database: {db_name}")
        print(f"{'='*80}")

        # Import this database using the shared store
        tree = import_sqlite(
            db_path,
            pattern=pattern,
            seed=seed,
            batch_size=batch_size,
            store=store,  # Use shared store
            verbose_batches=verbose_batches,
            tables_filter=tables_filter
        )

        # Store results
        root_hash = tree._hash_node(tree.root)
        results[db_name] = (tree, root_hash)

        print(f"\n{db_name} root hash: {root_hash}")

    # Print overall summary
    total_time = time.time() - total_start
    print(f"\n{'='*80}")
    print(f"Directory import complete!")
    print(f"{'='*80}")
    print(f"Total databases: {len(results)}")
    print(f"Total time: {total_time:.2f}s")
    print(f"\nStore statistics:")
    print(f"  Total nodes in shared store: {store.count_nodes():,}")

    if isinstance(store, CachedFSStore):
        cache_stats = store.get_cache_stats()
        print(f"\nShared cache statistics:")
        for key, value in cache_stats.items():
            print(f"  {key}: {value}")

        creation_stats = store.get_creation_stats()
        print(f"\nCumulative node creation across all databases:")
        print(f"  Total leaves: {creation_stats['total_leaves_created']:,}")
        print(f"  Total internals: {creation_stats['total_internals_created']:,}")

        print(f"\nSize distributions across all databases:")
        store.print_distributions(bucket_count=10)

    print(f"\nRoot hashes by database:")
    for db_name, (_, root_hash) in sorted(results.items()):
        print(f"  {db_name}: {root_hash}")

    return results


def dump_keys(store_spec, prefix, cache_size=None, reconstruct_rows=False, root_hash=None):
    """
    Dump all keys with a given prefix from the store.

    Args:
        store_spec: Store specification
        prefix: Key prefix to filter (e.g., '/d/buses', '/s/')
        cache_size: Cache size for cached stores
        reconstruct_rows: If True, reconstruct row dicts from schema for /d/ keys
        root_hash: Optional root hash to load specific tree version
    """
    from tree import ProllyTree

    print(f"Opening store: {store_spec}")
    store = create_store_from_spec(store_spec, cache_size=cache_size)

    # Count all nodes in store
    node_count = store.count_nodes()
    print(f"Store contains {node_count:,} total nodes")

    if node_count == 0:
        print("Store is empty")
        return

    # Create tree - if root_hash provided, load that tree
    tree = ProllyTree(pattern=0.0001, seed=42, store=store)

    if root_hash:
        # Load specific tree version
        print(f"Loading tree from root hash: {root_hash}")
        tree.root = store.get_node(root_hash)
        if not tree.root:
            print(f"Error: Root hash {root_hash} not found in store")
            return
    else:
        # Try to find a root by scanning - this is a heuristic
        # In practice, the user should provide the root hash
        print("Warning: No root hash provided, attempting to reconstruct tree...")
        print("For accurate results, provide --root-hash parameter")

    # Use the new items() generator
    print(f"Fetching keys with prefix: {prefix}")

    # If reconstructing rows, we need to load schemas first
    schemas = {}
    if reconstruct_rows and prefix.startswith('/d/'):
        for schema_key, schema_value in tree.items('/s/'):
            table_name = schema_key[3:]  # Remove '/s/' prefix
            schemas[table_name] = json.loads(schema_value)

    # Display results
    count = 0
    for key, value in tree.items(prefix):
        count += 1
        if count <= 100:  # Limit display to first 100
            if reconstruct_rows and key.startswith('/d/'):
                # Extract table name from key
                parts = key.split('/')
                table_name = parts[2] if len(parts) > 2 else None

                if table_name and table_name in schemas:
                    schema = schemas[table_name]
                    row_values = json.loads(value)
                    row_dict = dict(zip(schema['columns'], row_values))
                    print(f"{key} => {json.dumps(row_dict, separators=(',', ':'))}")
                else:
                    print(f"{key} => {value}")
            else:
                print(f"{key} => {value}")

    print(f"\nTotal: {count:,} keys found")
    if count > 100:
        print(f"(showing first 100, {count - 100:,} more keys omitted)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='ProllyTree SQLite Importer and Dumper',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import SQLite database(s) into ProllyTree',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Import single database to memory
  python sqlite_import.py import database.sqlite

  # Import single database to filesystem
  python sqlite_import.py import database.sqlite --store file:///tmp/prolly_data

  # Import directory of databases with shared cached store
  python sqlite_import.py import /path/to/db/directory --store cached-file:///tmp/shared_store --cache-size 1000

  # Import specific tables only
  python sqlite_import.py import database.sqlite --tables buses generators --store file:///tmp/prolly_data
        ''')

    import_parser.add_argument('path', help='Path to SQLite database file or directory')
    import_parser.add_argument('--pattern', type=float, default=0.0001,
                        help='Split pattern (default: 0.0001)')
    import_parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for rolling hash (default: 42)')
    import_parser.add_argument('--store', default=':memory:',
                        help='Store spec: :memory:, file:///path, cached-file:///path (default: :memory:)')
    import_parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for inserts (default: 1000)')
    import_parser.add_argument('--cache-size', type=int, default=None,
                        help='Cache size for cached stores (default: 1000)')
    import_parser.add_argument('--verbose-batches', action='store_true',
                        help='Show detailed statistics for every batch insert')
    import_parser.add_argument('--directory', action='store_true',
                        help='Import all SQLite files from a directory using a shared store')
    import_parser.add_argument('--tables', nargs='+', default=None,
                        help='Specific table names to import (default: all)')

    # Dump subcommand
    dump_parser = subparsers.add_parser('dump', help='Dump keys from ProllyTree store',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Dump all data keys for buses table
  python sqlite_import.py dump /d/buses --store file:///tmp/prolly_data

  # Dump all schemas
  python sqlite_import.py dump /s/ --store file:///tmp/prolly_data

  # Dump and reconstruct row objects
  python sqlite_import.py dump /d/buses --store file:///tmp/prolly_data --reconstruct
        ''')

    dump_parser.add_argument('prefix', help='Key prefix to dump (e.g., /d/buses, /s/)')
    dump_parser.add_argument('--store', required=True,
                        help='Store spec: file:///path, cached-file:///path')
    dump_parser.add_argument('--cache-size', type=int, default=None,
                        help='Cache size for cached stores')
    dump_parser.add_argument('--reconstruct', action='store_true',
                        help='Reconstruct row objects from schema (for /d/ keys)')
    dump_parser.add_argument('--root-hash', type=str, default=None,
                        help='Root hash of tree to dump (required for accurate dump)')

    args = parser.parse_args()

    if args.command == 'import':
        # Check if path is a directory or a file
        if args.directory or os.path.isdir(args.path):
            # Directory import with shared store
            results = import_directory(
                args.path,
                pattern=args.pattern,
                seed=args.seed,
                batch_size=args.batch_size,
                store_spec=args.store,
                cache_size=args.cache_size,
                verbose_batches=args.verbose_batches,
                tables_filter=args.tables
            )
        else:
            # Single file import
            tree = import_sqlite(
                args.path,
                pattern=args.pattern,
                seed=args.seed,
                batch_size=args.batch_size,
                store_spec=args.store,
                cache_size=args.cache_size,
                verbose_batches=args.verbose_batches,
                tables_filter=args.tables
            )
    elif args.command == 'dump':
        dump_keys(
            store_spec=args.store,
            prefix=args.prefix,
            cache_size=args.cache_size,
            reconstruct_rows=args.reconstruct,
            root_hash=args.root_hash
        )
    else:
        parser.print_help()
