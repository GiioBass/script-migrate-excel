import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()  # Carga las variables del archivo .env

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Leer el archivo Excel
excel_file = 'ruta/al/archivo.xlsx'
df = pd.read_excel(excel_file)

# Conexión a la base de datos
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Asume que la columna de código de producto se llama 'codigo_producto'
codigo_col = 'codigo_producto'
# Las columnas de fechas empiezan desde la segunda columna
fecha_cols = df.columns[1:]

for idx, row in df.iterrows():
    codigo = row[codigo_col]
    # Obtener el product_supplier_id
    cursor.execute("SELECT id FROM productos WHERE codigo = %s", (codigo,))
    result = cursor.fetchone()
    if not result:
        print(f'Producto no encontrado: {codigo}')
        continue
    product_supplier_id = result[0]
    
    for fecha in fecha_cols:
        precio = row[fecha]
        if pd.isna(precio):
            continue  # Saltar si no hay precio
        # Insertar en la tabla de precios
        cursor.execute("""
            INSERT INTO precios (product_supplier_id, price, date_price)
            VALUES (%s, %s, %s)
        """, (product_supplier_id, precio, fecha))
    conn.commit()

cursor.close()
conn.close()
print("Migración completada.")