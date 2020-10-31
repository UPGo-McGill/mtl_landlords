import numpy as np
import os
import pandas as pd
import sqlite3

def main():
    time = '20200709-135659'
    sql_folder = f'sql/{time}'
    
    full_data = pd.read_csv('data/uniteevaluationfonciere.csv')['MATRICULE83']
    success = []
    failed = []
    for file in os.listdir(sql_folder):
        if file.endswith('.sqlite'):
            rows = load_data(f'{sql_folder}/{file}', 0, 10000)
            ids = [r[0] for r in rows]
            success.extend(ids)
    print(len(set(full_data)))
    print(len(set(success)))
    ids_to_write = np.setdiff1d(set(full_data), set(success))
    print(len(ids_to_write))
    with open(f'logs/{time}/missing.txt', 'a') as f:
        print("Writing missing ids to file.")
        for row in ids:
            if row not in full_data:
                f.write(f'{row}\n')
        f.close()
    
def load_data(filename, low, amount):
    sqlite_file = filename
    print(sqlite_file)
    conn = sqlite3.connect(sqlite_file)
    cur = conn.cursor()
    high = low + amount
    all_rows = [item for item in 
                cur.execute(f'SELECT * FROM html_pages WHERE rowid >= {low} AND rowid <= {high}')]
    conn.close()
    return(all_rows)

if __name__ == "__main__":
    main()
