import requests
from bs4 import BeautifulSoup
import psycopg2

def extract_data(url):
    pages = requests.get(url=url)
    soup = BeautifulSoup(pages.content, 'html.parser')

    name = [name.text for name in soup.find_all('a', class_='title')]
    price = [price.text for price in soup.find_all('h4', class_='price float-end card-title pull-right')]
    description = [description.text for description in soup.find_all('p', class_='description')]

    rating = []
    for rate in soup.find_all('div', class_='ratings'):
        flag = 0
        for r in rate.find_all('span'):
            flag += 1
        rating.append(flag)
    
    return name, price, description, rating

url = 'https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops'
name, price, description, rating = extract_data(url=url)

def create_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="@Nairobi2003",
        host="localhost",
        port=5432
    )
    
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE DATABASE database_db")
        print("Database 'database_db' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print("Database 'database_db' already exists.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating database:", error)
    
    cursor.close()
    conn.close()

def load_to_postgres():
    create_database()

    conn = psycopg2.connect(
        dbname="database_db",
        user="postgres",
        password="@Nairobi2003",
        host="localhost",
        port=5432
    )
    
    cursor = conn.cursor()
    print("Connected to 'database_db' database successfully!")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_table (
                name varchar(250) NOT NULL,
                price varchar(250) NOT NULL,
                description varchar(250) NOT NULL,
                rating varchar(250) NOT NULL
            )
        """)
        print("Table 'data_table' created successfully.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating table:", error)

    insert_query = """INSERT INTO data_table (name, price, description, rating) VALUES (%s, %s, %s, %s)"""
    for i in range(len(name)):
        records = (name[i], price[i], description[i], rating[i])
        cursor.execute(insert_query, records)
    
    conn.commit()
    print("Data inserted successfully!")

    cursor.close()
    conn.close()
    print("Connection Closed!")

load_to_postgres()
