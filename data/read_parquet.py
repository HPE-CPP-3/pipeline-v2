#!/usr/bin/env python3
import argparse
import pandas as pd
import os
import sys

def read_parquet(path, head=None):
    """Reads a Parquet file and prints its content."""
    try:
        if os.path.isdir(path):
            print(f"Directory: {path}")
            files = [f for f in os.listdir(path) if f.endswith('.parquet')]
            if not files:
                print("No .parquet files found in directory.")
                return
            print(f"Found {len(files)} parquet files. Reading the first one...")
            path = os.path.join(path, files[0])
        
        print(f"Reading file: {path}")
        df = pd.read_parquet(path)
        
        print("\n--- Schema/Info ---")
        print(df.info())
        
        print("\n--- Content ---")
        if head:
            print(df.head(head))
        else:
            print(df)
            
    except Exception as e:
        print(f"Error reading parquet file: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Read and display Parquet file content.")
    parser.add_argument("path", help="Path to the .parquet file or directory containing .parquet files")
    parser.add_argument("--head", type=int, help="Number of rows to display (default: all)")
    
    args = parser.parse_args()
    
    read_parquet(args.path, args.head)

if __name__ == "__main__":
    main()
