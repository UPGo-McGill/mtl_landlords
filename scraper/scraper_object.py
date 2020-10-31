from captcha_solver.captcha_solver import *
from session_handling.session_handling import *
from scraper.matricule import *
from scraper.scrape import *
from scraper.sql import *

import math
import os
import requests
import sys
import time


class Scraper_Object:
    def __init__(self):
        self.failed_captcha = True
        while(self.failed_captcha):
            while True:
                self.session, self.response, self.headers, self.cookies = initialize_session()
                if self.response.status_code == 200:
                    break
            self.captcha_s = get_captchas(self.response, self.headers, self.cookies)
            self.failed_captcha = fail_captcha(self.session, self.headers, self.captcha_s)
        
def main():
    scraper = Scraper_Object()

if __name__ == "__main__":
    main()