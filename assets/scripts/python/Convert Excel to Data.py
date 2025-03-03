import pandas as pd
import pymysql
import numpy as np

file_path = r"E:\projest Top 100 Social Media Influencers 2024\youtube_data_from_python.csv"

df = pd.read_csv(file_path)

df.columns = df.columns.str.strip().str.lower().str.replace(r"[^a-zA-Z0-9_]", "", regex=True)

df.columns = [f"column_{i}" if col.strip() == "" else col for i, col in enumerate(df.columns)]

df = df.replace({np.nan: None})

print("📌 Final Column Names:", df.columns.tolist())

conn = pymysql.connect(
    host="localhost",
    user="root",
    password="123456789",
    database="testdb_2",
    cursorclass=pymysql.cursors.DictCursor
)

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS youtube_data")

columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
create_table_query = f"""
CREATE TABLE youtube_data (
    id INT AUTO_INCREMENT PRIMARY KEY, 
    {columns}
)
"""
cursor.execute(create_table_query)

placeholders = ", ".join(["%s"] * len(df.columns))
insert_query = f"INSERT INTO youtube_data ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({placeholders})"

data_to_insert = [tuple(row) for _, row in df.iterrows()]
cursor.executemany(insert_query, data_to_insert)

conn.commit()
cursor.close()
conn.close()

print("✅ Data Inserted Successfully!")
