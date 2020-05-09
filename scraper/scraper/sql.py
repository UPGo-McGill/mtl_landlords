import sqlite3

def create_table(sqlite_file, idx):
    conn = sqlite3.connect(f"{sqlite_file}_{idx}.sqlite")
    c = conn.cursor()
    c.execute('''CREATE TABLE html_pages
    ([id] TEXT PRIMARY KEY,[html] blob)''')
    conn = sqlite3.connect(f"{sqlite_file}_{idx}.sqlite")

def save_to_sql(sqlite_file, matricule, binary):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute("INSERT INTO html_pages (id, html) VALUES (?, ?)", (matricule, binary))
    conn.commit()
    conn.close()