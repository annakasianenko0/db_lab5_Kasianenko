import psycopg2
import matplotlib.pyplot as plt

def exec_query(query):
    conn = psycopg2.connect(database="lab3_bd",
                        user="postgres",
                        host='localhost',
                        password="jrnftlh",
                        port=5432)
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    finally:
        conn.close()

def fetch_rows(query):
    conn = psycopg2.connect(database="lab3_bd",
                        user="postgres",
                        host='localhost',
                        password="jrnftlh",
                        port=5432)
    try:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()


def create_view(name, query):
    exec_query(f"CREATE OR REPLACE VIEW {name} AS {query}")

query_1 = "SELECT c.name, avg(b.rating) FROM book b join category c on c.category_id=b.category_id group by c.name"
view_name_1 = "average_rating_by_category"
create_view(view_name_1, query_1)

query_2 = "SELECT c.name, count(b.book_id) * 100/ (SELECT count(*) from book) as percent FROM book b join category c on c.category_id=b.category_id group by c.name"
view_name_2 = "percent_of_books_by_category"
create_view(view_name_2, query_2)

query_3 = """SELECT a.name, avg(b.rating) rating FROM book b 
    join book_author ba on ba.book_id=b.book_id  
    join author a on a.author_id=ba.author_id
    group by a.name
    order by rating asc

"""
view_name_3 = "author_rating"
create_view(view_name_3, query_3)

ratings = []
categories = []
rows = fetch_rows(f"SELECT * FROM {view_name_1}")
for row in rows:
    ratings.append(row[1])
    categories.append(row[0])

plt.bar(categories, ratings)
plt.xlabel('Book category')
plt.ylabel('Average rating')
plt.savefig(f"{view_name_1}.png")
plt.show()
plt.close()


rows = fetch_rows(f"SELECT * FROM {view_name_2}")
vals = []
labels = []
for row in rows:
    labels.append(row[0])
    vals.append(row[1])

fig, ax = plt.subplots()
ax.pie(vals, labels=labels, autopct='%1.1f%%')
ax.axis("equal")
fig.savefig(f"{view_name_2}.png")
plt.show()
plt.close(fig)



ratings = []
authors = []
rows = fetch_rows(f"SELECT * FROM {view_name_3}")
for row in rows:
    ratings.append(row[1])
    authors.append(row[0])

plt.bar(authors, ratings)
plt.xlabel('Author')
plt.ylabel('Average rating')
plt.savefig(f"{view_name_3}.png")
plt.show()
plt.close()

