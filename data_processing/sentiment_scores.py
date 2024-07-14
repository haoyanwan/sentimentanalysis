import psycopg2

BATCH_SIZE = 100  # Number of posts to fetch per batch

def fetch_posts_batch(connection, last_id, batch_size):
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT id, content FROM posts WHERE sentiment_score IS NULL AND id > %s ORDER BY id ASC LIMIT %s",
            (last_id, batch_size)
        )
        posts = cursor.fetchall()
        cursor.close()
        return posts
    except Exception as e:
        print(f"Error fetching posts: {e}")
        return []

def update_sentiment_score(connection, post_id, sentiment_score):
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE posts SET sentiment_score = %s WHERE id = %s", (sentiment_score, post_id))
        connection.commit()
        cursor.close()
    except Exception as e:
        print(f"Error updating sentiment score: {e}")

def main():

    db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'Wonderful886',
        'host': 'localhost',
        'port': 5432
    }
    
    # Database connection and processing
    connection = None
    try:
        connection = psycopg2.connect(**db_params)
        last_id = 0
        
        while True:
            posts = fetch_posts_batch(connection, last_id, BATCH_SIZE)
            if not posts:
                break
            
            for post_id, content in posts:
                # Placeholder for sentiment analysis - replace this with your actual sentiment analysis logic
                sentiment_score = sentiment_analysis_pipeline(content)
                
                update_sentiment_score(connection, post_id, sentiment_score)
                last_id = post_id  # Update last_id to the latest processed post

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()

def sentiment_analysis_pipeline(content):
    # Placeholder function for sentiment analysis
    # Replace this with your actual sentiment scoring logic
    return 0.0

if __name__ == "__main__":
    main()
