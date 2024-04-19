from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import csv
from datetime import datetime, timedelta, date
import os
import psycopg2
import re
import tweepy
import locale
from babel.dates import format_date
from webdriver_manager.chrome import ChromeDriverManager
from decimal import Decimal
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import subprocess



# Increase the default script timeout
capabilities = DesiredCapabilities.CHROME.copy()
capabilities['pageLoadStrategy'] = 'eager'  # This can be 'eager' or 'none' if you want to be more aggressive


chrome_options = Options()
chrome_options.add_argument("--headless")  # Important for headless running
chrome_options.add_argument("--no-sandbox")  # Bypass OS security model, required for headless
chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")  # Applicable for headless running
chrome_options.add_argument('--disable-software-rasterizer')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
# Database connection parameters
dbname = 'postgres'
user = 'postgres'
password = 'Mamama00.'
host = 'database-1.cfke6mia4o8l.us-east-1.rds.amazonaws.com'
port= '5432'

#service = Service('/Users/ivanmanfredi/Downloads/chromedriver-mac-arm64/chromedriver')



# Function to read URLs from a file
def post_to_twitter(message):
    # Your credentials - replace these with your own
    consumer_key = "FtNaa3neJ19FmKfsqLW0gfcyH"
    consumer_secret = "ihJIXzAlrqk6TTMALjgay9ddeg9WXKAKxlazmoZwZwe8vHvZzq"
    access_token = "798548272256323585-a0bi8sYMSmsZ7lt6FXY5KWpRHZuMARt"
    access_token_secret = "VKnT52mdSbKCUw23F883EQaS7sjYIlWdU9k43VvIQWKyN"

    # Initialize Tweepy Client
    client = tweepy.Client(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret
    )

    try:
        # Create a tweet using Tweepy Client for Twitter API v2
        response = client.create_tweet(text=message)
        print(f"Tweet posted successfully. Tweet ID: {response.data['id']}")
    except tweepy.TweepyException as e:
        print(f"Error posting tweet: {e}")
def get_last_day_of_previous_month(date):
    """
    Returns the last day of the month before the given date.
    """
    first_day_of_current_month = date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month
def report_top_categories(conn, scrape_date):
    # Convert string date to datetime object
    date_object = datetime.strptime(scrape_date, '%Y-%m-%d')
    
    # Calculate the last day of the previous month as the start of the period
    start_of_period = get_last_day_of_previous_month(date_object)
    
    cur = conn.cursor()
    # Ensure the date used in the SQL query is in the correct format ('YYYY-MM-DD')
    sql_start_date = start_of_period.strftime('%Y-%m-%d')
    sql_scrape_date = scrape_date  # This should already be in 'YYYY-MM-DD' format

    price_increase_query = """
    WITH StartPrices AS (
        SELECT product_category, AVG(average_category_price) AS start_avg_price
        FROM product_prices
        WHERE scrape_date = %s
        GROUP BY product_category
    ), EndPrices AS (
        SELECT product_category, AVG(average_category_price) AS end_avg_price
        FROM product_prices
        WHERE scrape_date = %s
        GROUP BY product_category
    )
    SELECT StartPrices.product_category, 
           (EndPrices.end_avg_price - StartPrices.start_avg_price) / StartPrices.start_avg_price * 100 AS price_change_percentage
    FROM StartPrices
    JOIN EndPrices ON StartPrices.product_category = EndPrices.product_category
    ORDER BY price_change_percentage DESC
    LIMIT 3
    """
    cur.execute(price_increase_query, (sql_start_date, sql_scrape_date))
    price_increases = cur.fetchall()

    # Use Babel's format_date for display purposes only
    formatted_scrape_date = format_date(date_object, format='d MMMM', locale='es_ES')



    if price_increases:
        message = f"Categorías con mayor aumento de precios al {formatted_scrape_date}:\n"
        for product_category, price_change_percentage in price_increases:
            message += f"{product_category}: {price_change_percentage:.2f}%\n"
        
        print(message)
    else:
        print("No data available for price increases.")
def report_price_reductions(conn, scrape_date):
    # Convert string date to datetime object
    date_object = datetime.strptime(scrape_date, '%Y-%m-%d')
    
    # Calculate the last day of the previous month as the start of the period
    start_of_period = get_last_day_of_previous_month(date_object)
    
    cur = conn.cursor()
    # Ensure the date used in the SQL query is in the correct format ('YYYY-MM-DD')
    sql_start_date = start_of_period.strftime('%Y-%m-%d')
    sql_scrape_date = scrape_date  # This should already be in 'YYYY-MM-DD' format
    cur = conn.cursor()
    # Query to find the average_category_price at the start and end of the period for each category
    price_reduction_query = """
    WITH StartPrices AS (
        SELECT product_category, AVG(average_category_price) AS start_avg_price
        FROM product_prices
        WHERE scrape_date = %s
        GROUP BY product_category
    ), EndPrices AS (
        SELECT product_category, AVG(average_category_price) AS end_avg_price
        FROM product_prices
        WHERE scrape_date = %s
        GROUP BY product_category
    )
    SELECT StartPrices.product_category, 
           (EndPrices.end_avg_price - StartPrices.start_avg_price) / StartPrices.start_avg_price * 100 AS price_change_percentage
    FROM StartPrices
    JOIN EndPrices ON StartPrices.product_category = EndPrices.product_category
    ORDER BY price_change_percentage ASC
    LIMIT 3
    """
    cur.execute(price_reduction_query, (sql_start_date, sql_scrape_date))
    price_reductions = cur.fetchall()

    # Use Babel's format_date for display purposes only
    formatted_scrape_date = format_date(date_object, format='d MMMM', locale='es_ES')

    if price_reductions:
        message = f"Categorías con mayor reducción de precios al {formatted_scrape_date}:\n"
        for product_category, price_change_percentage in price_reductions:
            message += f"{product_category}: {price_change_percentage:.2f}%\n"
        print(message)  # Replace with your preferred method of output, e.g., logging or tweeting
    else:
        print("No data available for price reductions.")
def report_canasta_price_change(conn, scrape_date):
    # Convert string date to datetime object
    date_object = datetime.strptime(scrape_date, '%Y-%m-%d')
    
    # Calculate the last day of the previous month
    end_of_previous_month = (date_object.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Determine if today is the last day of the current month
    last_day_of_current_month = (date_object.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    
    cur = conn.cursor()
    # Query to fetch the total_canasta_value for the end of the previous month and the current scrape date
    canasta_value_query = """
    SELECT scrape_date, total_canasta_value
    FROM product_prices
    WHERE scrape_date IN (%s, %s)
    GROUP BY scrape_date, total_canasta_value
    ORDER BY scrape_date
    """
    # Execute the query with the end of the previous month and the current scrape date
    cur.execute(canasta_value_query, (end_of_previous_month, scrape_date))
    results = cur.fetchall()
    formatted_scrape_date = format_date(date_object, format='d MMMM', locale='es_ES')

    if results and len(results) == 2:
        # Assuming the first result is for the end of the previous month and the second is the current scrape date
        start_value = results[0][1]  # Total canasta value at the end of the previous month
        end_value = results[1][1]    # Total canasta value for the current scrape date
        
        # Calculate the accumulated monthly inflation rate
        inflation_rate = ((end_value - start_value) / start_value) * 100 if start_value else 0
        message = f"La variación acumulada de precios de la canasta al {formatted_scrape_date} es del {inflation_rate:.2f}%."
        
        if date_object == last_day_of_current_month:
            message += " Esta es la tasa final de variación de precios para el mes actual."
        print(message)    
    else:
        message = "Datos insuficientes para calcular la variación de la canasta."
        print(message)
    # Define the command you would run in Bash
    # Define your email and subject
    recipient = "ivan.manfredi2001@gmail.com"
    subject = "CARREBOT OF THE DAY"
    send_mail = f'echo "{message}" | mail -s "{subject}" {recipient}'
    subprocess.run(send_mail, shell=True)  
def report_weighted_canasta_price_change(conn, scrape_date):
    # Convert string date to datetime object
    date_object = datetime.strptime(scrape_date, '%Y-%m-%d')
    
    # Calculate the last day of the previous month
    end_of_previous_month = (date_object.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Determine if today is the last day of the current month
    last_day_of_current_month = (date_object.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    
    cur = conn.cursor()
    # Query to fetch the total_canasta_value for the end of the previous month and the current scrape date
    weighted_canasta_value_query = """
    SELECT scrape_date, weighted_total_canasta
    FROM product_prices
    WHERE scrape_date IN (%s, %s)
    GROUP BY scrape_date, weighted_total_canasta
    ORDER BY scrape_date
    """
    # Execute the query with the end of the previous month and the current scrape date
    cur.execute(weighted_canasta_value_query, (end_of_previous_month, scrape_date))
    results = cur.fetchall()
    formatted_scrape_date = format_date(date_object, format='d MMMM', locale='es_ES')

    if results and len(results) == 2:
        # Assuming the first result is for the end of the previous month and the second is the current scrape date
        start_value = results[0][1]  # Total canasta value at the end of the previous month
        end_value = results[1][1]    # Total canasta value for the current scrape date
        
        # Calculate the accumulated monthly inflation rate
        inflation_rate = ((end_value - start_value) / start_value) * 100 if start_value else 0
        message = f"La variación acumulada de la canasta WEIGHTED al {formatted_scrape_date} es del {inflation_rate:.2f}%."
        
        if date_object == last_day_of_current_month:
            message += " Esta es la tasa final de variación para el mes actual."
        print(message)    
    else:
        message = "Datos insuficientes para calcular la variación de la canasta."
        print(message)
    


def read_product_urls(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file]
def determine_product_cat(url):
    # Simplified logic for determining product type based on URL
    if 'arlistan' in url or 'cafe-instantaneo' in url:
        return "Café Instantáneo"
    elif 'te-en-saquitos' in url or 'te-morenita-' in url:
        return "Te en Saquitos"
    elif 'cacao-en-polvo' in url:
        return "Cacao en Polvo"
    elif 'yerba' in url:
        return "Yerba Mate"
    elif 'vino-tinto' in url:
        return "Vino Tinto"
    elif 'cerveza' in url:
        return "Cerveza"
    elif '-con-gas-' in url or 'soda' in url:
        return "Agua con Gas"
    elif 'jugo-' in url:
        return "Jugo Concentrado"
    elif '-cola-' in url:
        return "Gaseosa"
    elif 'caldo-' in url:
        return "Caldo Concentrado"
    elif 'vinagre-' in url:
        return "Vinagre"
    elif 'mayonesa-' in url:
        return "Mayonesa"
    elif 'sal-fina' in url:
        return "Sal Fina"
    elif 'mermelada' in url:
        return "Mermelada"
    elif 'dulce-de-batata' in url:
        return "Dulce de Batata"
    elif 'azucar-' in url:
        return "Azucar"
    elif 'lentejas-' in url:
        return "Lentejas"
    elif 'arvejas-' in url:
        return "Arvejas"
    elif '/tomate-perita-' in url:
        return "Tomate Enlatado"
    elif '/pan-' in url:
        return "Pan Lactal"
    elif 'galletitas-chocolinas' in url or 'galletitas-dulces' in url or 'galletitas-toddy' in url or 'galletitas-chocolate' in url:
        return "Galletita Dulces"
    elif '/galletitas-cerealitas' in url or '/galletitas-crackers-la-providencia' in url or 'galletitas-crackers-traviata'in url:
        return "Galletita de Agua"
    elif 'harina-de-trigo' in url:
        return "Harina de Trigo"
    elif 'arroz-' in url:
        return "Arroz"
    elif '/fideos-' in url:
        return "Pastas"
    elif '/asado-' in url:
        return "Asado"
    elif '/carnaza-' in url:
        return "Carnaza"
    elif '/carre-' in url:
        return "Carre de Cerdo"
    elif '/paleta-el' in url:
        return "Paleta Vaca"
    elif '/carne-picada-' in url or 'carne-icada-' in url:
        return "Carne Picada"
    elif '/milanesa-de-nalga-' in url:
        return "Nalga"
    elif '/pechito-de-cerdo-' in url:
        return "Pechito de Cerdo"
    elif '/pollo-entero-congelado-' in url:
        return "Pollo"
    elif '/filet-de-merluza-' in url:
        return "Filet de Merluza"
    elif '/mortadela-' in url:
        return "Mortadela"
    elif '/paleta-cocida' in url:
        return "Paleta Cocida"
    elif '/salchichon-' in url:
        return "Salchichon"
    elif '/salame-' in url:
        return "Salame"
    elif '/aceite-de-girasol-' in url:
        return "Aceite de Girasol"
    elif '/margarina-en-pan-' in url:
        return "Margarina"
    elif '/leche-ultra-entera-' in url or '/leche-multivitaminas-' in url:
        return "Leche Entera"
    elif '/leche-en-polvo-' in url:
        return "Leche en Polvo"
    elif '/queso-crema-' in url or '/queso-fundido-' in url:
        return "Queso untable"
    elif '/queso-cuartirolo-' in url:
        return "Queso Cuartirolo"
    elif '/queso-en-hebras-' in url or '/queso-rallado-' in url:
        return "Queso Rallado"
    elif '/manteca-' in url:
        return "Manteca"
    elif '/yogur-bebible-' in url:
        return "Yogur Bebible"
    elif '/dulce-de-leche-' in url:
        return "Dulce de Leche"
    elif '/huevo-' in url or '/huevos-blancos-' in url:
        return "Huevo"
    elif '/manzana-red-' in url:
        return "Manzana Roja"
    elif '/pera-' in url:
        return "Pera"
    elif '/batata-x-kg' in url:
        return "Batata"
    elif '/acelga-' in url:
        return "Acelga"
    elif '/cebolla' in url:
        return "Cebolla"
    elif '/choclo-en-granos' in url:
        return "Choclo en Granos"
    elif '/lechuga-' in url:
        return "Lechuga"
    elif '/tomate-x-kg' in url:
        return "Tomate"
    elif '/zapallo-' in url:
        return "Zapallo"
    elif '/higado-x-kg-' in url:
        return "Higado"
    
    

    else:
        return "Unknown Product Type"
def apply_category_weight(category):
    weights = {
    # High Importance
    "Pan Lactal": 1.5, "Arroz": 3, "Pollo": 6, "Huevo": 40, "Tomate": 3, "Acelga": 2, "Leche Entera": 10, "Zapallo": 17 , "Lechuga": 4, "Yogur Bebible": 5, "Pera": 4, "Manzana Roja": 5,
    "Lentejas": 5, "Arvejas": 7, "Cebolla": 6, "Choclo en Granos": 1.5, "Batata": 15, "Agua con Gas": 6, "Carnaza": 2, "Nalga": 1.3, "Queso Cuartirolo": 1.3,
    # Proteins
    "Filet de Merluza": 0.7, "Asado": 1.2, "Carre de Cerdo": 26, "Pechito de Cerdo": 30, "Paleta Vaca": 1.4, "Carne Picada": 3.5,
    # Standard Importance
    "Café Instantáneo": 0.25, "Yerba Mate": 2, "Aceite de Girasol": 6, "Sal Fina": 3, "Azucar": 6, "Manteca": 0.7, "Queso Rallado": 0.4, "Tomate Enlatado": 4, "Pastas": 3.5, "Queso untable": 1,
    # Processed Meats
    "Mortadela": 1.5, "Paleta Cocida": 0.5, "Salchichon": 0.5, "Salame": 0.3,
    # Less Essential or Healthier Options
    "Vino Tinto": 1.5, "Caldo Concentrado": 0.5, "Vinagre": 4, "Mayonesa": 2, "Galletita de Agua": 1.5, "Harina de Trigo": 10, "Margarina": 1.3,
    # Snacks and Sweets
    "Galletita Dulces": 0.7, "Dulce de Batata": 2.5, "Mermelada": 1.4, "Dulce de Leche": 1.4,
    # Specialty Beverages
    "Gaseosa": 7, "Jugo Concentrado": 1.6, "Te en Saquitos": 0.4, "Cerveza": 2, "Cacao en Polvo": 0.7,
    }
    return weights.get(category, 1)  # Default weight is 1 if category not listed
# Price format conversion function
def convert_price_format(price_str):
    # Check if the input is not a string, return it directly
    if not isinstance(price_str, str):
        return price_str
    
    # Proceed with the original logic if it's a string
    price_str = price_str.replace("$", "").strip()  # Remove the currency symbol and any spaces
    price_str = price_str.replace(".", "").replace(",", ".")  # Remove thousand separators and replace decimal comma
    return float(price_str)
def update_average_category_price(conn, scrape_date):
    cur = conn.cursor()
    
    # Update the average category price for the specified scrape_date
    update_average_price_query = """
    UPDATE product_prices p
    SET average_category_price = (
            SELECT AVG(price_per_kg)
            FROM product_prices
            WHERE product_category = p.product_category
            AND scrape_date = %s
        )
    WHERE scrape_date = %s;
    """
    cur.execute(update_average_price_query, (scrape_date, scrape_date))
    
    conn.commit()
def update_total_canasta_value(conn):
    cur = conn.cursor()
    today = date.today()
    
    # Update the total canasta value for today's scrape_date
    update_total_canasta_query = """
    UPDATE product_prices p
    SET total_canasta_value = (
        SELECT SUM(average_category_price)
        FROM (
            SELECT DISTINCT ON (product_category) average_category_price
            FROM product_prices
            WHERE scrape_date = %s
        ) AS subquery
    )
    WHERE scrape_date = %s
    """
    cur.execute(update_total_canasta_query, (today, today))
    
    conn.commit()
def update_weighted_average_prices(conn):
    cur = conn.cursor()
    today_date = date.today().strftime('%Y-%m-%d')
    cur.execute("SELECT product_category, average_category_price FROM product_prices WHERE scrape_date = %s", (today_date,))
    category_prices = cur.fetchall()
    
    update_query = """UPDATE product_prices SET weighted_average_category_price = %s WHERE product_category = %s AND scrape_date = %s"""
    
    for category, avg_price in category_prices:
        weight = apply_category_weight(category)  # Dynamically get the weight for each category
        # Ensure weight is a Decimal before multiplication
        weighted_avg_price = avg_price * Decimal(str(weight))
        
        cur.execute(update_query, (weighted_avg_price, category, today_date))
    
    conn.commit()
def update_total_canasta_value_weighted(conn):
    cur = conn.cursor()
    today = date.today()
    
    # Update the total canasta value for today's scrape_date
    update_total_canasta_weighted_query = """
    UPDATE product_prices
    SET weighted_total_canasta = (
        SELECT SUM(MaxWeightedPrice)
        FROM (
            SELECT product_category, MAX(weighted_average_category_price) AS MaxWeightedPrice
            FROM product_prices
            WHERE scrape_date = %s
            GROUP BY product_category
        ) AS WeightedPrices
    )
    WHERE scrape_date = %s;

    """
    cur.execute(update_total_canasta_weighted_query, (today, today))
    
    conn.commit()
def insert_into_db(product_data):
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    for data in product_data:
        product_name, price, price_per_kg, product_category, scrape_date = data
        
        # Check if price is not available (assuming None or empty string denotes unavailability)
        if not price:
            # Query to find the most recent price_per_kg for the product
            cur.execute("""
                SELECT price FROM product_prices
                WHERE product_name = %s AND scrape_date < %s
                ORDER BY scrape_date DESC
                LIMIT 1
            """, (product_name, scrape_date))
            result = cur.fetchone()
            # Use the most recent price if available
            if result:
                price = result[0]
            else:
                # Skip insertion if no price is available
                print(f"No available price for {product_name} on {scrape_date}, and no recent price found.")
                continue
        
        # Convert price and price_per_kg to float
        price = convert_price_format(price) if price else None
        price_per_kg = convert_price_format(price_per_kg)

        # Updated insert query to correctly match the provided placeholders
        insert_query = """
        INSERT INTO product_prices (product_name, price, price_per_kg, product_category, scrape_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_name, scrape_date) DO NOTHING;
        """
        
        # Insert the data
        cur.execute(insert_query, (product_name, price, price_per_kg, product_category, scrape_date))
    
    conn.commit()

    # After inserting, calculate and update the category inflation rates
    update_average_category_price(conn, scrape_date)
    update_total_canasta_value(conn)
    update_weighted_average_prices(conn)
    update_total_canasta_value_weighted(conn)
    report_top_categories(conn, scrape_date)
    report_price_reductions(conn, scrape_date)
    report_canasta_price_change(conn, scrape_date)
    report_weighted_canasta_price_change(conn, scrape_date)  


    cur.close()
    conn.close()

# Adjust this path to where you save your file
product_urls_file = '/home/ec2-user/carrefour-aws/product_urls.txt'
#product_urls_file = '/Users/ivanmanfredi/Desktop/Carrefour-aws/product_urls.txt'
product_urls = read_product_urls(product_urls_file)

# Current date 
scrape_date = datetime.now().strftime('%Y-%m-%d')

# Prepare a list to hold product data
products_data = []

from selenium.common.exceptions import NoSuchElementException, TimeoutException

products_data = []

for url in product_urls:
    try:
        product_cat = determine_product_cat(url)
        driver.get(url)
        print(url)
        time.sleep(5)  # Adjust sleep time based on page load times

        # Attempt to fetch the product name
        product_name_xpath = '//h1[contains(@class, "productNameContainer")]/span[contains(@class, "productBrand")]'
        product_name_element = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, product_name_xpath))
        )
        product_name = product_name_element.text
        product_price = ''  # Default value if price is not found as an empty string
        
        try:  # Attempt to fetch the price details
            price_element = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, '//*[contains(@class, "product-price-0-x-sellingPriceValue")]'))
            )
            if "c/u" in price_element.text or "%" in price_element.text:
                product_price_element = WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, '//*[@class="valtech-carrefourar-product-price-0-x-listPrice"]'))
                )
                product_price = product_price_element.text
            else:
                product_price = price_element.text
        except (NoSuchElementException, TimeoutException):
            print("This product is down, we will use last known price")
            # If price details are not found, leave product_price as an empty string
            pass

        try:  # Fetch price per kg, assuming it might not always be present
            product_price_per_kg = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, '//*[contains(@class, "dynamic-weight-price-0-x-currencyContainer")]'))
            ).text
        except (NoSuchElementException, TimeoutException):
            # Set as an empty string for missing price per kg data
            pass

        # Append data to products_data list
        products_data.append([product_name, product_price, product_price_per_kg, product_cat, scrape_date])

    except (NoSuchElementException, TimeoutException) as e:
        # Handle errors specifically related to critical product name fetching
        print(f"Error processing product name for URL {url}")
        continue  # Skip this URL and move to the next one


# Insert data into the database
insert_into_db(products_data)

# Clean up by closing the driver after all operations
driver.quit()