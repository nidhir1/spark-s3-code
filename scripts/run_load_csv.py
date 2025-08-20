# scripts/run_load_csv.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.load_csv import load_csvs

if __name__ == "__main__":
    bucket_name = "mybucket"
    
    print(f"Loading CSVs from bucket '{bucket_name}'...\n")
    all_dfs = load_csvs(bucket_name)
    
    if all_dfs:
        print("\nSchemas of loaded DataFrames:")
        for name, df in all_dfs.items():
            print(f"\n{name}:")
            df.printSchema()
    else:
        print("No DataFrames loaded.")
