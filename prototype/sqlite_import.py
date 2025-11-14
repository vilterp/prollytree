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
from prolly_tree import ProllyTree

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

def import_sqlite(db_path, pattern=0.0001, seed=42, batch_size=1000):
    """
    Import all tables from SQLite into ProllyTree.

    Args:
        db_path: Path to SQLite database
        pattern: ProllyTree split pattern (default 0.0001)
        seed: Random seed for rolling hash
        batch_size: Number of rows to insert per batch
    """
    print(f"Opening database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(tables)} tables: {', '.join(tables)}")

    # Initialize ProllyTree
    print(f"\nInitializing ProllyTree (pattern={pattern}, seed={seed})")
    tree = ProllyTree(pattern=pattern, seed=seed)

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
    print(f"  Total nodes in storage: {len(tree.nodes):,}")

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
    import sys

    if len(sys.argv) < 2:
        print("Usage: python sqlite_import.py <database.sqlite> [pattern] [seed]")
        print("\nExample:")
        print("  python sqlite_import.py BC00ALL-26SP.sqlite")
        print("  python sqlite_import.py BC00ALL-26SP.sqlite 0.0001 42")
        sys.exit(1)

    db_path = sys.argv[1]
    pattern = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0001
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 42

    tree = import_sqlite(db_path, pattern=pattern, seed=seed)
