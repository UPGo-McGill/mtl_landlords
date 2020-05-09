from captcha_solver.captcha_solver import *
from session_handling.session_handling import *
from scraper.matricule import *
from scraper.scrape import *
from scraper.sql import *

import sys

def main():
    scrape_individual()

def scrape_individual():
    session, response, headers, cookies = initialize_session()
    captcha_s = get_captchas(response, headers, cookies)
    pass_captcha(session, headers, captcha_s)
    ids_to_scrape = prepare_matricule([sys.argv[1]])
    status = fetch_url(ids_to_scrape[0], session)
    return status

if __name__ == "__main__":
    main()