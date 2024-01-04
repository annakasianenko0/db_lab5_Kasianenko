import pandas as pd
from sqlalchemy import create_engine




engine = create_engine("postgresql+psycopg2://postgres:jrnftlh@localhost:5432/lab3_bd")

with engine.connect() as conn, conn.begin():
    for table_name in ['book', 'author', 'category', 'format', 'book_author']:
        table_data = pd.read_sql_table(table_name, conn)
        table_data.to_json(f"{table_name}.json")

