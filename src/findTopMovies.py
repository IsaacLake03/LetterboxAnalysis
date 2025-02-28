import duckdb

# This code checks the movies.parquet file for the top 100 movies
# then prints their names and ratings
def find_top_movies():
    # Connect to the movies.parquet file
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE TABLE movies AS SELECT * FROM parquet_scan('../LetterboxParquet/movies.parquet')")
    
    # Get the top 100 movies
    result = con.execute("SELECT * FROM parquet_scan('../LetterboxParquet/movies.parquet') ORDER BY rating DESC LIMIT 100")
    
    # Schema of the movies table is as follows:
    # id, name, date, tagline, description, minute, rating
    
    # Print the top 100 movies
    for i, row in enumerate(result.fetchall()):
        print(f"{i + 1}. {row[1]} ({row[2]}) - Rating: {row[6]}")
        
if __name__ == "__main__":
    find_top_movies()