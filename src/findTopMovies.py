import duckdb

# This code checks the movies.parquet file for the top 100 movies
# then prints their names and ratings
def find_top_movies(genre):
    # Connect to the movies.parquet file
    con = duckdb.connect()
    
    # Get the top 10 movies of the specified genre
    query = f"""
    SELECT m.*
    FROM parquet_scan('/Users/isaac/Desktop/CSC369/LetterboxAnalysis/LetterboxParquet/movies.parquet') m
    JOIN parquet_scan('/Users/isaac/Desktop/CSC369/LetterboxAnalysis/LetterboxParquet/genres.parquet') g
    ON m.id = g.id
    WHERE g.genre = '{genre}' AND m.minute > 60 AND m.minute < 240
    AND m.id IN(
        SELECT id
        FROM parquet_scan('/Users/isaac/Desktop/CSC369/LetterboxAnalysis/LetterboxParquet/releases.parquet')
    )
    ORDER BY m.rating DESC
    LIMIT 10
    """
    result = con.execute(query)
    
    # Schema of the movies table is as follows:
    # id, name, date, tagline, description, minute, rating
    
    # Print the top 10 movies
    for i, row in enumerate(result.fetchall()):
        print(f"{i + 1}. {row[1]} ({row[2]}) - Rating: {row[6]}")

# Test Main
if __name__ == "__main__":
    genre = 'Adventure'
    find_top_movies(genre)