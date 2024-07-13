import time
from flask import Flask, jsonify
import psycopg2

# Define your database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Wonderful886',
    'host': 'localhost',
    'port': 5432
}

app = Flask(__name__)

@app.route('/api/time')
def get_current_time():
    return jsonify({'time': time.time()})

def get_stock_from_id(id):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM stocks WHERE id = %s", (id,))
        stock = cur.fetchone()
        if stock:
            return stock[1]
        else:
            return None
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()

def get_id_from_stock(stock):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM stocks WHERE ticker = %s", (stock,))
        stock = cur.fetchone()
        if stock:
            return stock[0]
        else:
            return None
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()

def get_stock_mentions(stock):
    try:
        stock_id = get_id_from_stock(stock)
        if stock_id is None:
            return None
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM poststocks WHERE stock_id = %s", (stock_id,))
        mentions = cur.fetchall()
        return mentions
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()

# Example usage
#print(get_stock_mentions('NVDA'))
# sample output [(5280698, 3103, 330,"2012-04-11 09:40:40"), (5280709, 3103, 331, "2012-04-11 09:40:40")]
# where the first element in each tuple is the post_id, the second element is the stock_id, and the third element is the density of the stock in the post, and the fourth element is the time the post was created

from datetime import datetime, timezone
def convert_timestamp_to_utc(ts):
    utc_time = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    #print(utc_time)
    return utc_time

def get_post_time(post_id):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM posts WHERE id = %s", (post_id,))
        post = cur.fetchone()
        if post:
            # convert unix seconds to datetime
            ts = post[6]
            return ts
        else:
            return None
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/api/stock/<stock>')
def get_stock_info(stock):
    mentions = get_stock_mentions(stock)
    if mentions is None:
        return jsonify({'error': 'Stock not found'})
    
    aggregated_data = {}
    for mention in mentions:
        unix_timestamp = mention[2]
        post_time_utc = datetime.utcfromtimestamp(unix_timestamp)
        date = post_time_utc.date().isoformat()  # Convert datetime object to date string (YYYY-MM-DD)

        density = 1
        if density == 0:
            continue
        
        if date in aggregated_data:
            aggregated_data[date] += density
        else:
            aggregated_data[date] = density

    data = [{'time': date, 'density': aggregated_data[date]} for date in aggregated_data.keys()]
    return jsonify(data)


# Start the Flask server
if __name__ == '__main__':
    app.run(debug=True)