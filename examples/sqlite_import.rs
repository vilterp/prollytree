/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! SQLite to ProllyTree Import Tool
//!
//! This tool imports SQLite databases into ProllyTree for performance analysis.
//! It's designed to help identify performance bottlenecks in the tree implementation.
//!
//! Usage:
//!   cargo run --example sqlite_import --release -- <sqlite_file> [options]
//!
//! Examples:
//!   # In-memory with default node sizes
//!   cargo run --example sqlite_import --release -- mydata.db
//!
//!   # In-memory with large nodes
//!   cargo run --example sqlite_import --release -- mydata.db --node-size large
//!
//!   # With profiling info
//!   cargo run --example sqlite_import --release -- mydata.db --verbose

use prollytree::{
    config::TreeConfig,
    storage::InMemoryNodeStorage,
    tree::{ProllyTree, Tree},
};
use rusqlite::{Connection, Result as SqliteResult};
use std::collections::HashMap;
use std::env;
use std::time::Instant;

#[derive(Debug)]
struct TableInfo {
    columns: Vec<String>,
    primary_keys: Vec<String>,
}

fn get_all_tables(conn: &Connection) -> SqliteResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;

    let tables = stmt
        .query_map([], |row| row.get(0))?
        .collect::<SqliteResult<Vec<String>>>()?;

    Ok(tables)
}

fn get_table_info(conn: &Connection, table_name: &str) -> SqliteResult<TableInfo> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))?;

    let mut columns = Vec::new();
    let mut primary_keys = Vec::new();

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(1)?, // column name
            row.get::<_, i64>(5)?,    // pk flag
        ))
    })?;

    for row in rows {
        let (col_name, pk_flag) = row?;
        columns.push(col_name.clone());
        if pk_flag > 0 {
            primary_keys.push(col_name);
        }
    }

    // If no primary key, use rowid
    if primary_keys.is_empty() {
        primary_keys.push("rowid".to_string());
    }

    Ok(TableInfo {
        columns,
        primary_keys,
    })
}

fn build_primary_key(row_data: &HashMap<String, String>, pk_cols: &[String]) -> String {
    if pk_cols.len() == 1 {
        row_data.get(&pk_cols[0]).unwrap().clone()
    } else {
        pk_cols
            .iter()
            .map(|col| row_data.get(col).unwrap())
            .cloned()
            .collect::<Vec<_>>()
            .join(":")
    }
}

fn import_table(
    conn: &Connection,
    tree: &mut ProllyTree<32, InMemoryNodeStorage<32>>,
    table_name: &str,
    batch_size: usize,
    verbose: bool,
) -> SqliteResult<usize> {
    let info = get_table_info(conn, table_name)?;

    if verbose {
        println!("\nImporting table: {}", table_name);
        println!("  Columns: {}", info.columns.join(", "));
        println!("  Primary key(s): {}", info.primary_keys.join(", "));
    }

    // Build SELECT query
    let mut select_cols = info.columns.clone();
    if info.primary_keys.contains(&"rowid".to_string())
        && !info.columns.contains(&"rowid".to_string())
    {
        select_cols.insert(0, "rowid".to_string());
    }

    let query = format!("SELECT {} FROM {}", select_cols.join(", "), table_name);
    let mut stmt = conn.prepare(&query)?;

    let mut rows_iter = stmt.query([])?;
    let mut total_rows = 0;
    let mut batch_keys = Vec::new();
    let mut batch_values = Vec::new();

    let batch_start = Instant::now();

    while let Some(row) = rows_iter.next()? {
        // Build row data map
        let mut row_data = HashMap::new();
        for (i, col_name) in select_cols.iter().enumerate() {
            let value: String = row.get(i).unwrap_or_else(|_| "".to_string());
            row_data.insert(col_name.clone(), value);
        }

        // Build key: /<table>/<primary_key>
        let pk_value = build_primary_key(&row_data, &info.primary_keys);
        let key = format!("/{}/{}", table_name, pk_value);

        // Build value: JSON array of column values
        let value_list: Vec<String> = info
            .columns
            .iter()
            .map(|col| {
                row_data
                    .get(col)
                    .map(|v| format!("\"{}\"", v.replace('"', "\\\"")))
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect();
        let value = format!("[{}]", value_list.join(","));

        batch_keys.push(key.into_bytes());
        batch_values.push(value.into_bytes());

        // Insert batch when it reaches batch_size
        if batch_keys.len() >= batch_size {
            tree.insert_batch(&batch_keys, &batch_values);
            total_rows += batch_keys.len();

            if verbose {
                let elapsed = batch_start.elapsed();
                let rate = total_rows as f64 / elapsed.as_secs_f64();
                let root_hash = tree.get_root_hash().unwrap_or_default();
                println!(
                    "  Inserted {} rows... ({:.1} rows/sec) Root: {:02x}{:02x}{:02x}{:02x}...",
                    total_rows,
                    rate,
                    root_hash.0[0],
                    root_hash.0[1],
                    root_hash.0[2],
                    root_hash.0[3]
                );
            }

            batch_keys.clear();
            batch_values.clear();
        }
    }

    // Insert remaining batch
    if !batch_keys.is_empty() {
        tree.insert_batch(&batch_keys, &batch_values);
        total_rows += batch_keys.len();
    }

    if verbose {
        let elapsed = batch_start.elapsed();
        let rate = total_rows as f64 / elapsed.as_secs_f64();
        println!(
            "  Inserted {} rows - DONE ({:.1} rows/sec)",
            total_rows, rate
        );
    }

    Ok(total_rows)
}

fn create_tree_config(node_size: &str) -> TreeConfig<32> {
    match node_size {
        "default" => TreeConfig::default(),
        "small" => TreeConfig {
            min_chunk_size: 100,
            max_chunk_size: 1_000_000,
            pattern: 0xFFFF, // ~1 in 65K split
            ..TreeConfig::default()
        },
        "medium" => TreeConfig {
            min_chunk_size: 500,
            max_chunk_size: 5_000_000,
            pattern: 0xFFFFF, // ~1 in 1M split
            ..TreeConfig::default()
        },
        "large" => TreeConfig {
            min_chunk_size: 1000,
            max_chunk_size: 10_000_000,
            pattern: 0xFFFFFF, // ~1 in 16M split
            ..TreeConfig::default()
        },
        "xlarge" => TreeConfig {
            min_chunk_size: 2000,
            max_chunk_size: 20_000_000,
            pattern: 0xFFFFFFF, // ~1 in 268M split
            ..TreeConfig::default()
        },
        _ => {
            eprintln!("Unknown node size: {}, using default", node_size);
            TreeConfig::default()
        }
    }
}

fn main() -> SqliteResult<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!(
            "Usage: {} <sqlite_file> [--node-size SIZE] [--batch-size N] [--verbose]",
            args[0]
        );
        eprintln!("\nOptions:");
        eprintln!("  --node-size SIZE    Node size: default, small, medium, large, xlarge (default: default)");
        eprintln!("  --batch-size N      Batch size for inserts (default: 1000)");
        eprintln!("  --verbose           Print detailed progress");
        eprintln!("\nExamples:");
        eprintln!("  {} mydata.db", args[0]);
        eprintln!("  {} mydata.db --node-size large --verbose", args[0]);
        std::process::exit(1);
    }

    let sqlite_file = &args[1];
    let mut node_size = "default";
    let mut batch_size = 1000;
    let mut verbose = false;

    // Parse arguments
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--node-size" => {
                if i + 1 < args.len() {
                    node_size = &args[i + 1];
                    i += 2;
                } else {
                    eprintln!("Error: --node-size requires a value");
                    std::process::exit(1);
                }
            }
            "--batch-size" => {
                if i + 1 < args.len() {
                    batch_size = args[i + 1].parse().unwrap_or(1000);
                    i += 2;
                } else {
                    eprintln!("Error: --batch-size requires a value");
                    std::process::exit(1);
                }
            }
            "--verbose" | "-v" => {
                verbose = true;
                i += 1;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
    }

    println!("Opening SQLite database: {}", sqlite_file);
    let conn = Connection::open(sqlite_file)?;

    println!("Getting table list...");
    let tables = get_all_tables(&conn)?;

    if tables.is_empty() {
        println!("No tables found in database");
        return Ok(());
    }

    println!("Tables to import: {}", tables.join(", "));

    println!("\nInitializing ProllyTree");
    println!("  Storage: in-memory");
    println!("  Node size: {}", node_size);
    println!("  Batch size: {}", batch_size);

    let config = create_tree_config(node_size);
    let storage = InMemoryNodeStorage::<32>::new();
    let mut tree = ProllyTree::new(storage, config);

    let total_start = Instant::now();
    let mut total_rows = 0;

    for table in &tables {
        match import_table(&conn, &mut tree, table, batch_size, verbose) {
            Ok(rows) => {
                total_rows += rows;
            }
            Err(e) => {
                eprintln!("Error importing table {}: {}", table, e);
            }
        }
    }

    let total_elapsed = total_start.elapsed();
    let overall_rate = total_rows as f64 / total_elapsed.as_secs_f64();

    println!("\n{}", "=".repeat(60));
    println!("Import complete!");
    println!("  Total rows imported: {}", total_rows);
    println!("  Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("  Overall rate: {:.1} rows/sec", overall_rate);
    println!("  Tree size: {}", tree.size());
    if let Some(root_hash) = tree.get_root_hash() {
        println!("  Root hash: {:x}", root_hash);
    }
    println!("{}", "=".repeat(60));

    Ok(())
}
