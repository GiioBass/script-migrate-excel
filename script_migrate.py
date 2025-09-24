import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

csv_file = 'errores_fk3.csv'
df = pd.read_csv(csv_file)

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

for idx, row in df.iterrows():
    product_supplier_id = int(row['products_supplier_id'])
    price = row['price']
    date_price = row['date_price']
    try:
        cursor.execute("""
            INSERT INTO historical_price (products_supplier_id, price, date_price)
            VALUES (%s, %s, %s)
        """, (product_supplier_id, price, date_price))
        conn.commit()  # Commit después de cada insert
        print(f"Row {idx}: Insert OK: id={product_supplier_id}, price={price}, date={date_price}")
    except Exception as e:
        print(f"Row {idx}: Insert ERROR: id={product_supplier_id}, price={price}, date={date_price} | Error: {e}")
cursor.close()
conn.close()

print("Importación a la base de datos completada.")