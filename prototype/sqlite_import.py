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
from prolly_tree import ProllyTree, create_store_from_spec

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def get_primary_key(cursor, table_name):
    """Get the primary key column for a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    for row in cursor.fetchall():
        if row[5]:  # pk column is at index 5
            return row[1]  # name is at index 1
    # If no primary key, use rowid
    return "rowid"

def import_sqlite(db_path, pattern=0.0001, seed=42, batch_size=1000, store_spec=':memory:'):
    """
    Import all tables from SQLite into ProllyTree.

    Args:
        db_path: Path to SQLite database
        pattern: ProllyTree split pattern (default 0.0001)
        seed: Random seed for rolling hash
        batch_size: Number of rows to insert per batch
        store_spec: Store specification (:memory:, file://path, s3://bucket)
    """
    print(f"Opening database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(tables)} tables: {', '.join(tables)}")

    # Initialize ProllyTree with specified store
    print(f"\nInitializing ProllyTree (pattern={pattern}, seed={seed}, store={store_spec})")
    store = create_store_from_spec(store_spec)
    tree = ProllyTree(pattern=pattern, seed=seed, store=store)

    total_rows = 0
    total_start = time.time()

    for table_name in tables:
        print(f"\n{'='*80}")
        print(f"Importing table: {table_name}")
        print(f"{'='*80}")

        # Get primary key
        pk_column = get_primary_key(cursor, table_name)
        print(f"Primary key: {pk_column}")

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Total rows: {row_count:,}")

        if row_count == 0:
            print("Skipping empty table")
            continue

        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]

        # Process in batches
        table_start = time.time()
        rows_processed = 0

        # Select with rowid explicitly
        if pk_column == "rowid":
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
                if pk_column == "rowid":
                    pk_value = row[0]
                    # Create row dict from remaining columns
                    row_dict = dict(zip(columns, row[1:]))
                else:
                    # Create row dict
                    row_dict = dict(zip(columns, row))
                    pk_value = row_dict[pk_column]

                # Create key-value pair
                key = f"/{table_name}/{pk_value}"
                value = json.dumps(row_dict, separators=(',', ':'))

                mutations.append((key, value))

            # Sort mutations by key (required for prolly tree)
            mutations.sort(key=lambda x: x[0])

            # Insert batch
            tree.insert_batch(mutations, verbose=False)

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

    # Verify we can read some data
    print(f"\nVerifying data...")
    result = tree.verify()
    print(f"  Total key-value pairs in tree: {len(result):,}")

    if len(result) > 0:
        print(f"  First key: {result[0][0]}")
        print(f"  Last key: {result[-1][0]}")

    conn.close()
    return tree

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Import SQLite database into ProllyTree',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Import to memory
  python sqlite_import.py database.sqlite

  # Import to filesystem
  python sqlite_import.py database.sqlite --store file:///tmp/prolly_data

  # Import with custom pattern and seed
  python sqlite_import.py database.sqlite --pattern 0.0001 --seed 42

  # Import to S3 (not yet implemented)
  python sqlite_import.py database.sqlite --store s3://my-bucket
        '''
    )

    parser.add_argument('database', help='Path to SQLite database file')
    parser.add_argument('--pattern', type=float, default=0.0001,
                        help='Split pattern (default: 0.0001)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for rolling hash (default: 42)')
    parser.add_argument('--store', default=':memory:',
                        help='Store spec: :memory:, file:///path, or s3://bucket (default: :memory:)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for inserts (default: 1000)')

    args = parser.parse_args()

    tree = import_sqlite(
        args.database,
        pattern=args.pattern,
        seed=args.seed,
        batch_size=args.batch_size,
        store_spec=args.store
    )
