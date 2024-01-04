import pandas as pd
import psycopg2

def exec_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    

def fetch_rows(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def generate_pk_value(conn, table_name, column_name):
    max = fetch_rows(conn, f"SELECT MAX({column_name}) FROM {table_name}")[0][0]
    if max == None:
        return 1
    else:
        return max + 1


conn = psycopg2.connect(database="lab3_bd",
                    user="postgres",
                    host='localhost',
                    password="jrnftlh",
                    port=5432)

try:
    print("Truncating tables ..")
    exec_query(conn, "TRUNCATE book_author, book, author, format, category")

    print("Reading data ..")
    df = pd.read_csv("main_dataset.csv")
    data_len = len(df)
    print(f"{data_len} lines read..")
    print("Processig data..")
    for i in range(data_len):
        book_name = df.iloc[i, 1]
        author_name = df.iloc[i, 2]
        book_format = df.iloc[i, 3]
        book_rating = df.iloc[i, 4]
        book_isbn = df.iloc[i, 8]
        category_name = df.iloc[i, 9]

        c = fetch_rows(conn, f"SELECT COUNT(*) FROM book where isbn='{book_isbn}'")[0][0]
        if c > 0:
            continue # skipping multiple book entries in the dataset as in our db book can be in only 1 category
        next_book_id = generate_pk_value(conn, 'book', 'book_id')
        # Lookup author
        row = fetch_rows(conn, f"SELECT author_id from author where name=$$'{author_name}'$$")
        if len(row) > 0:
            author_id = row[0][0]
        else:
            author_id = generate_pk_value(conn, 'author', 'author_id')
            exec_query(conn, f"INSERT INTO author (author_id, name) VALUES({author_id}, $$'{author_name}'$$)")
        # Lookup format
        row = fetch_rows(conn, f"SELECT format_id from format where name='{book_format}'")
        if len(row) > 0:
            format_id = row[0][0]
        else:
            format_id = generate_pk_value(conn, 'format', 'format_id')
            exec_query(conn, f"INSERT INTO format (format_id, name) VALUES({format_id}, '{book_format}')")
        # Lookup category
        row = fetch_rows(conn, f"SELECT category_id from category where name='{category_name}'")
        if len(row) > 0:
            category_id = row[0][0]
        else:
            category_id = generate_pk_value(conn, 'category', 'category_id')
            exec_query(conn, f"INSERT INTO category (category_id, name) VALUES({category_id}, '{category_name}')")

        exec_query(conn, f"""INSERT INTO book (book_id, name, isbn, rating, format_id, category_id) 
        VALUES({next_book_id}, $$'{book_name}'$$, '{book_isbn}', {book_rating}, {format_id}, {category_id})""")
        
        exec_query(conn, f"INSERT INTO book_author (book_id, author_id) VALUES({next_book_id}, {author_id})")
        
        conn.commit()
finally:
    conn.close()
    


