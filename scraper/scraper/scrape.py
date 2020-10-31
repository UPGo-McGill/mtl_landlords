import os

from itertools import repeat
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from scraper.sql import *
import concurrent.futures

def fetch_url_multi(ids_to_scrape, session, db_file, timestr,
                    url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"
                    ):
    matricule, data = ids_to_scrape
    response = session.post(url, data = data)
    status = True
    if "Rechercher par" not in response.text:
        print(f"Successfully opened page for ID {matricule}.")
        binary = response.text.encode('utf8')
        if os.path.isfile(db_file):
            save_to_sql(db_file, matricule, binary)
            print(f"Saved ID {matricule} to db.")
            with open(f"logs/{timestr}/scraped.txt", "a") as f:
                f.write(f"{matricule}\n")
    else:
        print("Failed to load page for ID {matricule}. Check if session expired.")
        status = False
        with open(f"logs/{timestr}/failed.txt", "a") as f:
            f.write(f"{matricule}\n")
    return status

def fetch_url(ids_to_scrape, session,
              url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"
              ):
    matricule, data = ids_to_scrape
    response = session.post(url, data = data)
    status = True
    if "Rechercher par" not in response.text:
        print(f"Successfully opened page for ID {matricule}.")
        binary = response.text.encode('utf8')
        with open(f"htmls/{matricule}.html", "wb") as f:
            f.write(binary)
            print(f"Saved ID {matricule} as HTML.")
    else:
        print("Failed to load page for ID. Check if session expired.")
        status = False
    return status

def thread(ids_to_scrape, session, db_file, timestr,
      url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"
     ):
    iterable = zip(ids_to_scrape, repeat(session), repeat(db_file), repeat(timestr), repeat(url))
    # with ThreadPool(30) as pool:
    #     status = pool.starmap(fetch_url_multi, iterable)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        status = executor.map(fetch_url_multi, ids_to_scrape)
    return list(status)

        
def make_dirs(timestr):
    os.makedirs(f'logs/{timestr}')
    os.makedirs(f'sql/{timestr}')