import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import csv

load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

csv_file = 'errores_fk5.csv'

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

batch_size = 100
chunk_size = 50000

row_counter = 0  # contador global

# Archivo para guardar lotes que fallan
error_file = open("errores_fk.csv", "w", newline="", encoding="utf-8")
error_writer = csv.writer(error_file)
error_writer.writerow(["fila_inicio", "fila_fin", "products_supplier_id", "price", "date_price", "error"])

for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
    data = []
    first_row = row_counter
    last_row = row_counter + len(chunk) - 1
    print(f"\n📌 Procesando filas {first_row} → {last_row}")

    for _, row in chunk.iterrows():
        row_counter += 1
        product_supplier_id = int(row['products_supplier_id'])
        price = row['price']
        date_price = row['date_price']
        data.append((product_supplier_id, price, date_price))

        if len(data) >= batch_size:
            try:
                cursor.executemany("""
                    INSERT INTO historical_price (products_supplier_id, price, date_price)
                    VALUES (%s, %s, %s)
                """, data)
                conn.commit()
                print(f"   ✅ {len(data)} filas insertadas (batch OK).")
            except Exception as e:
                print(f"   ❌ ERROR en batch filas {row_counter-len(data)} → {row_counter}: {e}")
                for d in data:
                    error_writer.writerow([row_counter-len(data), row_counter, *d, str(e)])
            data = []

    # Insertar lo que quede
    if data:
        try:
            cursor.executemany("""
                INSERT INTO historical_price (products_supplier_id, price, date_price)
                VALUES (%s, %s, %s)
            """, data)
            conn.commit()
            print(f"   ✅ {len(data)} filas insertadas (último batch del chunk).")
        except Exception as e:
            print(f"   ❌ ERROR en último batch filas {row_counter-len(data)} → {row_counter}: {e}")
            for d in data:
                error_writer.writerow([row_counter-len(data), row_counter, *d, str(e)])

cursor.close()
conn.close()
error_file.close()

print("\n🚀 Importación completada")
print(f"👉 Última fila procesada: {row_counter}")
print("⚠️ Revisa errores_fk.csv para ver los registros fallidos.")
