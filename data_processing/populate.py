import csv
import os
import json
import enchant
import psycopg2
import numpy as np
from collections import defaultdict
from tqdm import tqdm

def parse_csv_to_hashmap(file_path):
    stocks = defaultdict(dict)
    
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)  # Read the header row
        
        for row in csv_reader:
            symbol = row[0]
            stocks[symbol] = {
                'Company Name': row[1],
                'Industry': row[2],
                'Market Cap': row[3],
            }
    
    return stocks

def filter_stocks(stock_data, blacklist, whitelist):
    d = enchant.Dict("en_US")

    # Convert blacklist and whitelist to uppercase
    blacklist = [word.upper() for word in blacklist]
    whitelist = [word.upper() for word in whitelist]

    # Initialize a new hashmap for filtered stocks
    filtered_stocks = {}

    # Filter stocks based on the conditions
    for stock, data in stock_data.items():
        stock_upper = stock.upper()
        if len(stock_upper) > 1 and stock_upper not in blacklist and (stock_upper in whitelist or not d.check(stock_upper)):
            filtered_stocks[stock] = data

    return filtered_stocks

def process_file(directory, content_type, cursor, connection, stock_ids, filtered_stocks):
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
            for line in tqdm(file, desc=f"Processing {filename}", unit='lines', position=0, leave=True):
                process_line(line, content_type, cursor, connection, stock_ids, filtered_stocks)

def process_line(line, content_type, cursor, connection, stock_ids, filtered_stocks):
    try:
        data = json.loads(line)
        body = data.get('selftext', '') if content_type == 'Post' else data.get('body', '')
        if body == '':
            return
        created_utc = data.get('created_utc', '')
        ups = data.get('ups', 0)
        downs = data.get('downs', 0)
        virality = ups + downs

        cursor.execute("""
        INSERT INTO Posts (content, content_type, source, author, virality, date, sentiment_score, due_diligence, meme, result, target_audience, fear_and_greed_index)
        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL, 'Retail', NULL)
        RETURNING id
        """, (body, content_type, 'Social Media', 'Individual', virality, created_utc))
        post_id = cursor.fetchone()[0]
        connection.commit()

        # only get unique words from body
        unique_body = set(body.split())
        for word in unique_body:
            if word in filtered_stocks:
                cursor.execute("""
                INSERT INTO PostStocks (post_id, stock_id, date)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """, (post_id, stock_ids[word], created_utc))
                connection.commit()

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error processing line: {e}")

def main():
    # File paths and database parameters
    file_path = 'stocks-list.csv'
    directory_sub = 'C:\\Users\\haoyan\\Documents\\subreddits23\\wallstreetbets_submissions'
    directory_com = 'C:\\Users\\haoyan\\Documents\\subreddits23\\wallstreetbets_comments'
    db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'Wonderful886',
        'host': 'localhost',
        'port': 5432
    }
    
    # Blacklist and whitelist
    blacklist = ["YOU", "ARE", "IT", "OP", "ALL", "AI", "GO", "FOR", "A", "ON", "BY", "AT", "SO", "DD", "RH", "UP", "ME", "DO", "BE", "U", "LOVE", "AM", "OR", "F", "TD", "V", "R", "SO", "CAN","E", "C", "T","UK","TV", "NOW", "IQ", "EU", "B", "BIG", "OUT","AN","DAY","GOOD","ANY", "HAS","LINK","HUGE","RSI","PT","VS","S","VERY", "D","LIVE","FREE","REAL", "O","M","SEE","BACK", "WAY","NEXT","AS","RC", "DM","PR","OPEN","APP","BEST","POST","PLAY","LOW","OI","GAME","LOT","MA","NYC","AKA","HE", "ES","PLUG","RUN","GOLD","TOP","IRS","IP", "FCF","MRNA","CASH","EVER","HD","IMO","SAVE", "MOVE","SOS","GAIN","PAY","MAN","UI","NET", "PSA","WOLF"]
    whitelist = ['AMD', 'BABA', 'DIS', 'GE', 'GM', 'IQ', 'SQ', 'BA', 'JD', 'UPS', 'PLUG', 'META', 'LNG', 'ROKU', 'SHOP', 'IBM', 'EA', 'LULU', 'MARA', 'MGM', 'CVS', 'RIOT', 'GILD', 'GS', 'DNA', 'BP', 'VALE', 'COST', 'SENS', 'NET', 'SNOW', 'WOLF', 'KO', 'ROOT', 'DASH', 'CAT', 'SPOT', 'LAC', 'FOX', 'DB', 'PENN', 'DG', 'PINS', 'HSBC', 'FORD', 'WYNN', 'GPS', 'RYAN', 'ADP', 'ERIC', 'WEN', 'WM', 'HOG', 'EBAY', 'PG', 'DELL', 'SD', 'LEN', 'XXII', 'YUM', 'BIO', 'TAP', 'BARK', 'CARA', 'FIZZ', 'PEP', 'USB', 'ALLY', 'STEM', 'HIMS', 'DAWN', 'FICO', 'YETI', 'FIGS', 'ASTI', 'SAM', 'ZS', 'BR', 'CAKE', 'BAND', 'SOAR', 'EBON', 'MAPS', 'MARX', 'OMER', 'MFA', 'ZIP', 'SONY', 'SST', 'YELP', 'DOLE', 'SJW', 'WATT', 'MAMA', 'RICK', 'LODE', 'NSC', 'BTU', 'BLK', 'ASTR', 'BEAM', 'AMT', 'AXON', 'ALB', 'SAGE', 'ELF', 'BANC', 'GOLF', 'KOS', 'LEVI', 'ARC', 'IBEX', 'BIRD', 'ZION', 'ARCH', 'BURL', 'AZO', 'CUBE', 'FARM', 'OKE', 'LPG', 'HUBS', 'ATOM', 'TROW', 'CRUS', 'JAZZ', 'DLR', 'FROG', 'DADA', 'MESA', 'KIRK', 'ORLY', 'CHEF', 'CARR', 'GREE', 'LOCO', 'NOVA', 'VTOL', 'ROK', 'ARES', 'POLA', 'ADC', 'BAX', 'WPM', 'RELY', 'BOOT', 'ELAN', 'SWIM', 'FURY', 'ORAN', 'GUTS', 'SILK', 'GLUE', 'SLAM', 'ZETA', 'TWIN', 'PLOW', 'OUST', 'OVID', 'DOCS', 'CONN', 'ARLO', 'LOPE', 'ZEUS', 'GRAF', 'HOWL', 'ETON', 'ATEN', 'GILT', 'CERE', 'CERO', 'SKYE', 'LUCY', 'AGIO', 'ALTO', 'SURG', 'FLUX', 'SANA', 'EXPO', 'KURA', 'ACTG', 'FARO', 'CALX', 'SANG', 'BOLT', 'ERIE', 'COCO', 'VERA']

    # Parse CSV and filter stocks
    stock_data = parse_csv_to_hashmap(file_path)
    filtered_stocks = filter_stocks(stock_data, blacklist, whitelist)
    
    # Ensure filtered_stocks is a set of stock symbols
    filtered_stocks = set(filtered_stocks.keys())
    
    # Database connection and processing
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        stock_ids = {symbol: idx for idx, symbol in enumerate(filtered_stocks)}  # Assuming stock_ids are generated here
        
        process_file(directory_sub, 'Post', cursor, connection, stock_ids, filtered_stocks)
        process_file(directory_com, 'Comment', cursor, connection, stock_ids, filtered_stocks)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
