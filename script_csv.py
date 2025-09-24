import pandas as pd

# Cambia el nombre del archivo aquí si usas .ods
ods_file = 'excel.ods'  # Cambia por el nombre real de tu archivo ODS
df = pd.read_excel(ods_file, header=0, engine='odf')

product_id_col = 2
fecha_start = 4
fecha_end = 216
fechas = df.columns[fecha_start:fecha_end+1]

csv_rows = []

for idx in range(len(df)):
    row = df.iloc[idx]
    product_id_val = row.iloc[product_id_col]
    if pd.isna(product_id_val):
        continue  # Salta filas sin ID
    product_id = int(product_id_val)
    for col_idx, fecha in enumerate(fechas, start=fecha_start):
        precio = row.iloc[col_idx]
        if pd.isna(precio):
            precio = 0
        csv_rows.append([product_id, precio, fecha])

csv_df = pd.DataFrame(csv_rows, columns=['products_supplier_id', 'price', 'date_price'])
csv_df.to_csv('precios_importar.csv', index=False)

print("Archivo precios_importar.csv creado para importar.")