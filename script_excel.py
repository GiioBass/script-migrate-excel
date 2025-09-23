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

excel_file = 'excel2.xlsm'

# Leer el archivo con la fila 1 como cabecera
df = pd.read_excel(excel_file, header=0)

# Columna C (índice 2) tiene los códigos
codigo_col = 2

# Columnas L (índice 11) a HR (índice 220)
fecha_start = 11
fecha_end = 220

# Fechas están en la cabecera (fila 1), pandas las pone en df.columns
fechas = df.columns[fecha_start:fecha_end+1]

# Conexión a la base de datos
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

errores = []

# Iterar por todas las filas del DataFrame
for idx in range(len(df)):
    row = df.iloc[idx]
    codigo = row.iloc[codigo_col]  # Acceso por índice   
    cursor.execute("""
        SELECT products_suppliers.id
        FROM products_suppliers
        JOIN master_products ON master_products.id = products_suppliers.master_product_id
        WHERE products_suppliers.code = %s AND master_products.supplier_id = 3
        LIMIT 1
    """, (codigo,))
    result = cursor.fetchone()
    cursor.fetchall()  # Limpia resultados pendientes
    if not result:
        errores.append(str(codigo))
        print(f'Producto no encontrado: {codigo}')  # Mostrar por consola
        continue
    product_supplier_id = result[0]

    for col_idx, fecha in enumerate(fechas, start=fecha_start):
        precio = row.iloc[col_idx]
        if pd.isna(precio):
            precio = 0  # Guardar como 0 si la celda está vacía
        cursor.execute("""
            INSERT INTO historical_price (products_supplier_id, price, date_price)
            VALUES (%s, %s, %s)
        """, (product_supplier_id, precio, fecha))
    conn.commit()
    
    # Mostrar punto en la terminal
    print('.', end='', flush=True)

cursor.close()
conn.close()

# Guardar los códigos con error en un archivo de texto
with open('errores_productos.txt', 'w') as f:
    for code in errores:
        f.write(f"{code}\n")

print("Migración completada. Códigos con error guardados en errores_productos.txt.")