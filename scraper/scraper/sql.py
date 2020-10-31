import sqlite3

def create_table(db_file):
    conn = sqlite3.connect(db_file, timeout=15)
    c = conn.cursor()
    c.execute('''CREATE TABLE html_pages
    ([id] TEXT PRIMARY KEY,[html] blob)''')
    conn.commit()
    conn.close()

def save_to_sql(db_file, matricule, binary):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    #matricule = id_To_matricule(id_to_scrape)
    c.execute("INSERT INTO html_pages (id, html) VALUES (?, ?)", (matricule, binary))
    conn.commit()
    conn.close()
def save_batch_to_sql(db_file, data):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    for item in data:
        #print(item[0],[item[1]])
        c.execute("INSERT INTO html_pages (id, html) VALUES (?, ?)", (item[0], item[1]))
    conn.commit()
    conn.close()
def select_entries_with_distinct_ids(db_file):
    conn = sqlite3.connect(db_file, timeout=15)
    c = conn.cursor()
    data = c.execute("SELECT DISTINCT id, html FROM html_pages LIMIT 10")
    conn.close()

def select_all(db_file):
    conn = sqlite3.connect(db_file, timeout=15)
    c = conn.cursor()
    c.execute("SELECT DISTINCT id FROM html_pages")
    print(c.fetchall())
    conn.close()
    
def get_tables(db_file):
    conn = sqlite3.connect(db_file, timeout=15)
    c = conn.cursor()
    c.execute('SELECT name from sqlite_master where type= "table"')

    print(c.fetchall())
    conn.close()