#!/usr/bin/env python3
"""
Script to drop all columns from a table except those specified in a file.
Usage: python drop_columns.py <database_path> <table_name> [--batch-size=N] [--backup]
        python scripts/drop_columns.py datasrc/cac-combined.db affiliates --batch-size=10000 --backup --columns-file=fixtures/affiliates_columns.txt
"""

import sqlite3
import sys
import os
import time
import shutil
import argparse
from datetime import datetime

def read_columns_to_keep(file_path):
    """Read the list of columns to keep from a file."""
    with open(file_path, 'r') as f:
        columns = [line.strip() for line in f if line.strip()]
    return columns

def get_current_columns(cursor, table_name):
    """Get all columns in the specified table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]  # Column name is at index 1

def create_backup(db_path):
    """Create a backup of the database file."""
    backup_path = f"{db_path}.backup-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        shutil.copy2(db_path, backup_path)
        print(f"Created backup at: {backup_path}")
        return True
    except Exception as e:
        print(f"Failed to create backup: {str(e)}")
        return False

def count_rows(cursor, table_name):
    """Count the number of rows in a table."""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]
 
def drop_unused_columns(db_path, table_name, columns_to_keep, batch_size=5000, create_backup_file=False):
    """
    Drop all columns from a table except those specified.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to modify
        columns_to_keep: List of column names to keep
        batch_size: Number of rows to process in each batch
        create_backup_file: Whether to create a backup of the database before proceeding
    """
    # Create a backup if requested
    if create_backup_file:
        if not create_backup(db_path):
            print("Aborting operation due to backup failure.")
            return False
    
    # Set a higher timeout for large operations (default is 5 seconds)
    conn = sqlite3.connect(db_path, timeout=600)
    cursor = conn.cursor()
    
    # Enable foreign keys if they're being used
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    # Check if table exists
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        print(f"Error: Table '{table_name}' does not exist in the database.")
        conn.close()
        return False
    
    # Get current columns in the table
    current_columns = get_current_columns(cursor, table_name)
    print(f"Current columns in {table_name}: {current_columns}")
    
    # Filter columns_to_keep to only include columns that actually exist
    valid_columns = [col for col in columns_to_keep if col in current_columns]
    print(f"Columns to keep: {valid_columns}")
    
    # Columns to drop
    columns_to_drop = [col for col in current_columns if col not in columns_to_keep]
    print(f"Columns to drop: {columns_to_drop}")
    
    if not columns_to_drop:
        print("No columns to drop.")
        conn.close()
        return True
    
    # Count total rows
    total_rows = count_rows(cursor, table_name)
    print(f"Total rows to process: {total_rows:,}")
    
    # Create a new table with only the columns to keep
    column_defs = []
    # Get all column information at once to reduce queries
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = {row[1]: row for row in cursor.fetchall()}  # Map column names to their info
    
    for col in valid_columns:
        if col in table_info:
            row = table_info[col]
            col_name = row[1]
            col_type = row[2]  # Data type is at index 2
            not_null = "NOT NULL" if row[3] == 1 else ""  # NOT NULL constraint is at index 3
            default_val = f"DEFAULT {row[4]}" if row[4] is not None else ""  # Default value is at index 4
            is_pk = "PRIMARY KEY" if row[5] == 1 else ""  # Primary key flag is at index 5
            column_defs.append(f"{col_name} {col_type} {not_null} {default_val} {is_pk}".strip())
    
    # Get primary key info
    cursor.execute(f"PRAGMA table_info({table_name})")
    primary_key_cols = [row[1] for row in cursor.fetchall() if row[5] > 0]  # Primary key columns
    
    # Generate and execute SQL for creating new table and copying data
    try:
        # Start with turning off autocommit
        conn.isolation_level = 'DEFERRED'
        
        # Create a new table with only the columns we want to keep
        new_table = f"{table_name}_new"
        create_stmt = f"CREATE TABLE {new_table} ({', '.join(column_defs)})"
        print(f"Creating new table with statement: {create_stmt}")
        cursor.execute(create_stmt)
        
        # Get a column to use for batching (prefer primary key, otherwise use first column)
        batch_column = primary_key_cols[0] if primary_key_cols and primary_key_cols[0] in valid_columns else valid_columns[0]
        
        # Process data in batches
        print(f"Starting data migration in batches of {batch_size:,} rows")
        print(f"Using column '{batch_column}' for batching")
        
        start_time = time.time()
        processed_rows = 0
        
        # First get min and max values for batching
        cursor.execute(f"SELECT MIN({batch_column}), MAX({batch_column}) FROM {table_name}")
        min_id, max_id = cursor.fetchone()
        
        if min_id is not None and max_id is not None:
            current_min = min_id
            
            while current_min <= max_id:
                current_max = current_min + batch_size - 1
                
                # Begin transaction for this batch
                conn.execute("BEGIN TRANSACTION")
                
                # Copy data for this batch
                batch_insert_stmt = f"""
                INSERT INTO {new_table} 
                SELECT {', '.join(valid_columns)} FROM {table_name} 
                WHERE {batch_column} >= ? AND {batch_column} <= ?
                """
                cursor.execute(batch_insert_stmt, (current_min, current_max))
                
                # Get number of rows inserted in this batch
                batch_rows = cursor.rowcount if cursor.rowcount >= 0 else 0
                processed_rows += batch_rows
                
                # Commit this batch
                conn.commit()
                
                # Update progress
                percent_complete = (processed_rows / total_rows) * 100 if total_rows > 0 else 0
                elapsed_time = time.time() - start_time
                rows_per_sec = processed_rows / elapsed_time if elapsed_time > 0 else 0
                est_remaining = (total_rows - processed_rows) / rows_per_sec if rows_per_sec > 0 else 0
                
                print(f"Progress: {processed_rows:,}/{total_rows:,} rows ({percent_complete:.2f}%) - {rows_per_sec:.1f} rows/sec - Est. remaining: {est_remaining:.1f} seconds", end='\r')
                
                # Move to next batch
                current_min = current_max + 1
        
        print("\nFinished copying data to new table")
        
        # Begin final transaction
        conn.execute("BEGIN TRANSACTION")
        
        # Drop the old table
        drop_stmt = f"DROP TABLE {table_name}"
        print(f"Dropping old table: {drop_stmt}")
        cursor.execute(drop_stmt)
        
        # Rename the new table to the original name
        rename_stmt = f"ALTER TABLE {new_table} RENAME TO {table_name}"
        print(f"Renaming new table: {rename_stmt}")
        cursor.execute(rename_stmt)
        
        # Commit the final transaction
        conn.commit()
        
        # Calculate and display metrics
        total_time = time.time() - start_time
        print(f"Successfully dropped {len(columns_to_drop)} columns from {table_name}.")
        print(f"Processed {processed_rows:,} rows in {total_time:.2f} seconds ({processed_rows/total_time:.1f} rows/sec)")
        
    except Exception as e:
        # If anything goes wrong, rollback
        conn.rollback()
        print(f"Error occurred: {str(e)}")
        conn.close()
        return False
        
    # Re-enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    conn.close()
    return True

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Drop unused columns from a SQLite database table")
    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument("table_name", help="Name of the table to modify")
    parser.add_argument("--batch-size", type=int, default=5000, 
                        help="Number of rows to process in each batch (default: 5000)")
    parser.add_argument("--backup", action="store_true", 
                        help="Create a backup of the database before making changes")
    parser.add_argument("--columns-file", 
                        help="Path to the file containing columns to keep (default: fixtures/affiliates_columns.txt)")
    
    args = parser.parse_args()
    
    # Check if database file exists
    if not os.path.isfile(args.db_path):
        print(f"Error: Database file '{args.db_path}' does not exist.")
        sys.exit(1)
    
    # Path to the columns file
    if args.columns_file:
        columns_file = args.columns_file
    else:
        columns_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                  "fixtures", "affiliates_columns.txt")
    
    # Check if columns file exists
    if not os.path.isfile(columns_file):
        print(f"Error: Columns file '{columns_file}' does not exist.")
        sys.exit(1)
    
    print(f"Starting process with batch size: {args.batch_size:,}")
    if args.backup:
        print("Will create a backup before proceeding")
    
    # Read columns to keep
    print(f"Reading columns to keep from: {columns_file}")
    columns_to_keep = read_columns_to_keep(columns_file)
    
    # Drop unused columns
    success = drop_unused_columns(args.db_path, args.table_name, columns_to_keep, 
                                 batch_size=args.batch_size, create_backup_file=args.backup)
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
