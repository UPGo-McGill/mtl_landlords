import sqlite3

def create_table(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''CREATE TABLE html_pages
    ([id] TEXT PRIMARY KEY,[html] blob)''')
    conn = sqlite3.connect(db_file)

def save_to_sql(db_file, matricule, binary):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("INSERT INTO html_pages (id, html) VALUES (?, ?)", (matricule, binary))
    conn.commit()
    conn.close()