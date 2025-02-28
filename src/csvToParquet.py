import duckdb
import os

# This code converts the entire dataset from CSV to Parquet format
# The data is already well organized thus we can convert at in one
# go without altering the the storage methods or data. The code reads 
# the CSV file into a DuckDB table and then writes the table to a 
# Parquet file. The code loops through all CSV files in the input 
# folder and converts them to Parquet files in the output folder.

def csv_to_parquet(input_csv, output_parquet):
    try:
        # Connect to an in-memory DuckDB instance
        con = duckdb.connect(database=':memory:')

        # Read the CSV file into a DuckDB table and write it to a Parquet file
        con.execute(f"COPY (SELECT * FROM read_csv_auto('{input_csv}'), strict_mode=false, AUTO_DETECT=TRUE) TO '{output_parquet}' (FORMAT 'PARQUET', CODEC 'ZSTD')")
        print(f"Converted {input_csv} to {output_parquet}")
    except Exception as e:
        print(f"Error converting {input_csv} to {output_parquet}: {e}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_folder = os.path.join(script_dir, '../LetterboxData')
    output_folder = os.path.join(script_dir, '../LetterboxParquet')

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Loop through all CSV files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            input_csv = os.path.join(input_folder, filename)
            output_parquet = os.path.join(output_folder, filename.replace('.csv', '.parquet'))
            csv_to_parquet(input_csv, output_parquet)

