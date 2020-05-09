from captcha_solver.captcha_solver import *
from session_handling.session_handling import *
from scraper.matricule import *
from scraper.scrape import *
from scraper.sql import *

import sys

def main():
    scrape_batch()

def scrape_batch():
    session, response, headers, cookies = initialize_session()
    captcha_s = get_captchas(response, headers, cookies)
    pass_captcha(session, headers, captcha_s)
    matricule_df = load_matricule_numbers()
    if len(sys.argv) > 1:
        batch_size = int(sys.argv[1])
        start = int(sys.argv[2])
        stop = int(sys.argv[3])
    else:
        batch_size = 50000
        start = 0
        stop = len(matricule_df)
    for i in range(start, stop, batch_size):
        idx = min(i + batch_size, stop)
        create_table("sql/mtl_properties", f"{i}_{idx}")
        db_file = f"sql/mtl_properties_{i}_{idx}.sqlite"
        ids_to_scrape = prepare_matricule(matricule_df.MATRICULE83[i:idx])
        thread(ids_to_scrape, session, db_file)

if __name__ == "__main__":
    main()