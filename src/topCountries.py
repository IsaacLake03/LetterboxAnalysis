import duckdb

# This code checks the movies.parquet file for the top 100 movies
# then prints their names and ratings
def find_top_movies():
    # Connect to the movies.parquet file
    con = duckdb.connect()
    
    # Get the top 100 movies
    result = con.execute("SELECT country, count(*) as movies FROM parquet_scan('/Users/isaac/Desktop/CSC369/LetterboxAnalysis/LetterboxParquet/countries.parquet') GROUP BY country ORDER BY COUNT(*) DESC LIMIT 10")
    
    # Schema of the movies table is as follows:
    # id, name, date, tagline, description, minute, rating
    
    # Print the top 100 movies
    for i, row in enumerate(result.fetchall()):
        print(f"{i + 1}. {row[0]} - Number of Movies: {row[1]}")

# Test Main
if __name__ == "__main__":
    find_top_movies()