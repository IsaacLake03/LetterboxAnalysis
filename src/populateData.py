import duckdb

# This code checks the movies.parquet file to populate the data for the specified movies
# then prints their names, release dates, ratings, and studios
def get_movie_details(movie_titles):
    # Connect to the movies.parquet file
    con = duckdb.connect()
    
    # Create a parameterized query to avoid issues with special characters
    placeholders = ', '.join(['?'] * len(movie_titles))
    
    # Get the details for the specified movies
    query = f"""
    SELECT m.name, m.date, m.rating, s.studio
    FROM (
        SELECT name, date, rating, id
        FROM parquet_scan('/Users/isaac/Desktop/CSC369/LetterboxAnalysis/LetterboxParquet/movies.parquet')
        WHERE name IN ({placeholders})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY name ORDER BY rating DESC) = 1
    ) m
    JOIN (
        SELECT DISTINCT ON (id) id, studio
        FROM parquet_scan('/Users/isaac/Desktop/CSC369/LetterboxAnalysis/LetterboxParquet/studios.parquet')
    ) s
    ON m.id = s.id
    """
    result = con.execute(query, movie_titles)
    
    # Print the details for the specified movies
    for row in result.fetchall():
        print(f"Movie: {row[0]} ({row[1]}) - Rating: {row[2]} - Studio: {row[3]}")

# List of updated movie titles
updated_movie_titles = [
    "Leon", "12 Angry Men", "Taxi Driver", "Goodfellas",
    "Schindler's List", "Requiem for a Dream", "A Beautiful Mind", "GoldenEye",
    "Die Hard with a Vengeance", "The Rock", "Team America: World Police",
    "Dr. Strangelove", "Ace Ventura: Pet Detective", "Airplane!", "Shaun of the Dead", "Anchorman: The Legend of Ron Burgundy", 
    "National Lampoon's Vacation", "The Lost Boys", "28 Days Later", "Jacob's Ladder", "Shaun of the Dead", "The Shining",
    "The Notebook", "Eternal Sunshine of the Spotless Mind", "When Harry Met Sally...", "Four Weddings and a Funeral",
    "The Princess Bride", "Ghost"
]

final_titles = [
    "Die Hard: With a Vengeance", "Léon: The Professional", "Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb",
    "Scorsese's Goodfellas" 
]

# Get the details for the specified movies
get_movie_details(final_titles)