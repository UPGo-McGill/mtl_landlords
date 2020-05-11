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
    timestr = time.strftime("%Y%m%d-%H%M%S")
    max_restart = 10
    restart = 0
    make_dirs(timestr)
    if len(sys.argv) > 1:
        batch_size = int(sys.argv[1])
        start = int(sys.argv[2])
        stop = int(sys.argv[3])
    else:
        batch_size = 10000
        start = 0
        stop = len(matricule_df)
    subset_size = 100
    batches = range(start, stop, batch_size)
    idx = 0
    success = False
    print(f"Initializing session with batch size {batch_size} and upper limit {stop}.")
    while success == False and restart <= max_restart:
        break_outer = False
        session, response, headers, cookies = initialize_session()
        captcha_s = get_captchas(response, headers, cookies)
        pass_captcha(session, headers, captcha_s)
        batches = batches[idx:]
        for idx, lower in enumerate(batches):
            print(f"PROCESSING BATCH {idx} FROM {math.ceil(stop / batch_size)}.")
            upper = min(lower + batch_size, stop)
            db_file = f"sql/{timestr}/mtl_properties_{lower}_{upper}.sqlite"
            create_table(db_file)
            ids_to_scrape = prepare_matricule(ids = matricule_df.MATRICULE83[lower:upper])
            for subset_low in range(0, len(ids_to_scrape), subset_size):
                status = thread(ids_to_scrape[subset_low:subset_low+subset_size], session, db_file, timestr)
                if status.count(False) > (subset_size / 2):
                    print(f"Reinitializing at batch nr {idx}, because more than half of the properties failed.")
                    os.remove(db_file)
                    restart += 1
                    success = False
                    break_outer = True
                    break
                else:
                    success = True
                    continue
            if break_outer:
                break

if __name__ == "__main__":
    main()