from captcha_solver.captcha_solver import *
from session_handling.session_handling import *
from scraper.matricule import *
from scraper.scrape import *
from scraper.sql import *

import math
import os
import sys
import time

def main():
    scrape_batch()

def scrape_batch():
    matricule_df = load_matricule_numbers()
    ids = prepare_matricule(ids = matricule_df.MATRICULE83)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    make_dirs(timestr)
    if len(sys.argv) > 1:
        batch_size = int(sys.argv[1])
        start = int(sys.argv[2])
        stop = int(sys.argv[3])
    else:
        batch_size = 10000
        start = 0
        stop = len(matricule_df)
    batches = range(start, stop, batch_size)
    idx = 0
    success = False
    print(f"Initializing session with batch size {batch_size} and upper limit {stop}.")
    while True:
        session, response, headers, cookies = initialize_session()
        if response.status_code == 200:
            break
    captcha_s = get_captchas(response, headers, cookies)
    pass_captcha(session, headers, captcha_s)
    db_file = f"sql/{timestr}/mtl_properties.sqlite"
    create_table(db_file)
    batches = batches[idx:]
    for idx, lower in enumerate(batches):
        print(f"PROCESSING BATCH {idx} FROM {math.ceil(stop / batch_size)}.")
        upper = min(lower + batch_size, stop)
        ids_to_scrape = ids[lower:upper]
        status = thread(ids_to_scrape, session, db_file, timestr)
        if status.count(False) > 50:
            break
            print("Too many fails in status count.")
                                          
if __name__ == "__main__":
    main()