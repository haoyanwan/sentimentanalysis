import csv
from collections import defaultdict
import enchant
import psycopg2

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

    filtered_stocks = {}
    
    for stock, data in stock_data.items():
        stock_upper = stock.upper()
        if len(stock_upper) > 1 and stock_upper not in blacklist and (stock_upper in whitelist or not d.check(stock_upper)):
            filtered_stocks[stock] = data
    
    return filtered_stocks

def create_and_populate_tables(filtered_stocks, db_params):
    drop_tables_sql = """
    DROP TABLE IF EXISTS Posts CASCADE;
    DROP TABLE IF EXISTS Stocks CASCADE;
    DROP TABLE IF EXISTS PostStocks CASCADE;
    """
    
    create_tables_sql = """
    CREATE TABLE Stocks (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL
    );

    CREATE TABLE Posts (
        id SERIAL PRIMARY KEY,
        content TEXT NOT NULL,
        content_type VARCHAR(50),
        source VARCHAR(50),
        author VARCHAR(50),
        virality INT,
        date INT,
        sentiment_score FLOAT,
        due_diligence BOOLEAN,
        meme BOOLEAN,
        result BOOLEAN,
        target_audience VARCHAR(50),
        fear_and_greed_index FLOAT
    );

    CREATE TABLE PostStocks (
        post_id INT REFERENCES Posts(id) ON DELETE CASCADE,
        stock_id INT REFERENCES Stocks(id) ON DELETE CASCADE,
        date INT, 
        PRIMARY KEY (post_id, stock_id)
    );
    """
    
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        cursor.execute(drop_tables_sql)
        connection.commit()

        cursor.execute(create_tables_sql)
        connection.commit()

        stock_ids = {}
        for ticker in filtered_stocks.keys():
            cursor.execute("INSERT INTO Stocks (ticker) VALUES (%s) RETURNING id;", (ticker,))
            stock_id = cursor.fetchone()
            if stock_id:
                stock_ids[ticker] = stock_id[0]

        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    
    return stock_ids

def main():
    file_path = 'stocks-list.csv'
    db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'Wonderful886',
        'host': 'localhost',
        'port': 5432
    }
    
    blacklist = ["YOU", "ARE", "IT", "OP", "ALL", "AI", "GO", "FOR", "A", "ON", "BY", "AT", "SO", "DD", "RH", "UP", "ME", "DO", "BE", "U", "LOVE", "AM", "OR", "F", "TD", "V", "R", "SO", "CAN","E", "C", "T","UK","TV", "NOW", "IQ", "EU", "B", "BIG", "OUT","AN","DAY","GOOD","ANY", "HAS","LINK","HUGE","RSI","PT","VS","S","VERY", "D","LIVE","FREE","REAL", "O","M","SEE","BACK", "WAY","NEXT","AS","RC", "DM","PR","OPEN","APP","BEST","POST","PLAY","LOW","OI","GAME","LOT","MA","NYC","AKA","HE", "ES","PLUG","RUN","GOLD","TOP","IRS","IP", "FCF","MRNA","CASH","EVER","HD","IMO","SAVE", "MOVE","SOS","GAIN","PAY","MAN","UI","NET", "PSA","WOLF"]
    whitelist = ['AMD', 'BABA', 'DIS', 'GE', 'GM', 'IQ', 'SQ', 'BA', 'JD', 'UPS', 'PLUG', 'META', 'LNG', 'ROKU', 'SHOP', 'IBM', 'EA', 'LULU', 'MARA', 'MGM', 'CVS', 'RIOT', 'GILD', 'GS', 'DNA', 'BP', 'VALE', 'COST', 'SENS', 'NET', 'SNOW', 'WOLF', 'KO', 'ROOT', 'DASH', 'CAT', 'SPOT', 'LAC', 'FOX', 'DB', 'PENN', 'DG', 'PINS', 'HSBC', 'FORD', 'WYNN', 'GPS', 'RYAN', 'ADP', 'ERIC', 'WEN', 'WM', 'HOG', 'EBAY', 'PG', 'DELL', 'SD', 'LEN', 'XXII', 'YUM', 'BIO', 'TAP', 'BARK', 'CARA', 'FIZZ', 'PEP', 'USB', 'ALLY', 'STEM', 'HIMS', 'DAWN', 'FICO', 'YETI', 'FIGS', 'ASTI', 'SAM', 'ZS', 'BR', 'CAKE', 'BAND', 'SOAR', 'EBON', 'MAPS', 'MARX', 'OMER', 'MFA', 'ZIP', 'SONY', 'SST', 'YELP', 'DOLE', 'SJW', 'WATT', 'MAMA', 'RICK', 'LODE', 'NSC', 'BTU', 'BLK', 'ASTR', 'BEAM', 'AMT', 'AXON', 'ALB', 'SAGE', 'ELF', 'BANC', 'GOLF', 'KOS', 'LEVI', 'ARC', 'IBEX', 'BIRD', 'ZION', 'ARCH', 'BURL', 'AZO', 'CUBE', 'FARM', 'OKE', 'LPG', 'HUBS', 'ATOM', 'TROW', 'CRUS', 'JAZZ', 'DLR', 'FROG', 'DADA', 'MESA', 'KIRK', 'ORLY', 'CHEF', 'CARR', 'GREE', 'LOCO', 'NOVA', 'VTOL', 'ROK', 'ARES', 'POLA', 'ADC', 'BAX', 'WPM', 'RELY', 'BOOT', 'ELAN', 'SWIM', 'FURY', 'ORAN', 'GUTS', 'SILK', 'GLUE', 'SLAM', 'ZETA', 'TWIN', 'PLOW', 'OUST', 'OVID', 'DOCS', 'CONN', 'ARLO', 'LOPE', 'ZEUS', 'GRAF', 'HOWL', 'ETON', 'ATEN', 'GILT', 'CERE', 'CERO', 'SKYE', 'LUCY', 'AGIO', 'ALTO', 'SURG', 'FLUX', 'SANA', 'EXPO', 'KURA', 'ACTG', 'FARO', 'CALX', 'SANG', 'BOLT', 'ERIE', 'COCO', 'VERA']

    stock_data = parse_csv_to_hashmap(file_path)
    filtered_stocks = filter_stocks(stock_data, blacklist, whitelist)
    stock_ids = create_and_populate_tables(filtered_stocks, db_params)

    # Optional: Print the filtered stocks
    for stock, data in filtered_stocks.items():
        print(f'{stock}: {data}')

if __name__ == "__main__":
    main()
