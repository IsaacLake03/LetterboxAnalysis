import duckdb
import os

def csv_to_parquet(input_csv, output_parquet):
    # Connect to an in-memory DuckDB instance
    con = duckdb.connect(database=':memory:')

    # Read the CSV file into a DuckDB table
    con.execute(f"COPY (SELECT * FROM read_csv_auto('{input_csv}')) TO '{output_parquet}' (FORMAT 'parquet')")

if __name__ == "__main__":
    input_folder = '../LetterboxData'  # Replace with your input folder path
    output_folder = '../LetterboxParquet'  # Replace with your desired output folder path

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Loop through all CSV files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            input_csv = os.path.join(input_folder, filename)
            output_parquet = os.path.join(output_folder, filename.replace('.csv', '.parquet'))
            csv_to_parquet(input_csv, output_parquet)
            print(f"Converted {input_csv} to {output_parquet}")

