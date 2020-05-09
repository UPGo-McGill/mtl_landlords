import os

from itertools import repeat
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from scraper.sql import *

def fetch_url_multi(ids_to_scrape, session, db_file,
                    url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"
                    ):
    matricule, data = ids_to_scrape
    response = session.post(url, data = data)
    if "Rechercher par" not in response.text:
        print(f"Successfully opened page for ID {matricule}.")
        binary = response.text.encode('utf8')
        if os.path.isfile(db_file):
            save_to_sql(db_file, matricule, binary)
            print(f"Saved ID {matricule} to db.")
    else:
        print("Failed to load page for ID. Check if session expired.")

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

def thread(ids_to_scrape, session, db_file,
      url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/obtenirMatriculesPourNumero"
     ):
    iterable = zip(ids_to_scrape, repeat(session), repeat(db_file), repeat(url))
    with ThreadPool(cpu_count() - 1) as pool:
        pool.starmap(fetch_url_multi, iterable)