#!/usr/bin/python
# -*- coding:utf-8 -*-

import pytesseract
import requests
import sys

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from itertools import product
from os import walk
from PIL import Image

import re

def get_captcha_idx(path = "img"):
    regex = re.compile(r'\d+')
    f = [n for n in walk(path)][0][2]
    idxs = []
    for n in f:
        try:
            idxs.append(int(re.findall('\d+', n)[0]))
        except:
            pass
    return max(idxs)
        
def save_captcha(response, headers, cookies, idx,
                 baseURL = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/",
                 folder = "img",
                 filename = "captcha",
                 ext = "jpg"):
    soup = BeautifulSoup(response.text, 'html.parser')
    captcha_img = soup.find("img")
    image_url = baseURL + captcha_img["src"]
    img_response = requests.get(image_url, headers = headers, cookies = cookies)
    if img_response.status_code == 200:
        save_as = f"{folder}/{filename}_{idx+1}.{ext}"
        print(f"Request for captcha succeeded with response code {img_response.status_code}.\nCaptcha successfully saved as '{save_as}'.")
        with open(save_as, 'wb') as f:
            f.write(img_response.content)
    else:
        print(f"Request for captcha failed with response code {img_response.status_code}.")
    return img_response.status_code
        
def denoise(image):
    image = image.convert("LA")
    pixel_matrix = image.load()
    for col in range(0, image.height):
        for row in range(0, image.width):
            if pixel_matrix[row, col][0] > 50:
                pixel_matrix[row, col] = 0 
    im_grey = image.convert("L")
    for col in range(0, im_grey.height):
        for row in range(0, im_grey.width):
            if pixel_matrix[row, col] == (34,255):
                pixel_matrix[row, col] = (0,0)
            else:
                pixel_matrix[row, col] = (255,255)
    return im_grey

def recognize_captcha(im):
    # 1. threshold the image
    threshold = 1
    table = []
    for i in range(256):
        if i > threshold:
            table.append(0)
        else:
            table.append(1)
    out = im.point(table, '1')
    # 2. recognize with tesseract
    num = pytesseract.image_to_string(out, lang='eng', config='--psm 11')
    return num

def solve_captcha(filename, folder):
    image = Image.open(f"{folder}/{filename}")
    image = denoise(image)
    num = recognize_captcha(image)
    strs = list(num)
    return strs        
    
# Code adapted from https://stackoverflow.com/questions/52382444/replace-combinations-of-characters
def make_patterns(seq, keyletters = ["I", "J"]):
    permutations = []
    indices = [i for i, c in enumerate(seq) if c in keyletters]
    for t in product(keyletters, repeat=len(indices)):
        for i, c in zip(indices, t):
            seq[i] = c
        permutations.append(''.join(seq))
    return permutations

def get_captchas(response, headers, cookies, filename_base = "captcha", folder = "img"):
    try:
        idx = get_captcha_idx()
    except:
        idx = 0
    img_respone = save_captcha(response, headers, cookies, idx)

    if img_respone == 200:
        captcha = solve_captcha(filename = f"{filename_base}_{idx + 1}.jpg", folder = "img")
        captcha_s = make_patterns(captcha, ["I", "J"])
        print("Captcha(s) decoded are:")
        for c in captcha_s:
            print(c)
        return captcha_s
    else:
        print("Failed to open and save captcha image.")
        return []

def pass_captcha(session, headers, captcha_s, 
                 url = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/typeRecherche"):
    for c in captcha_s:
        print(f"Captcha to try is {c}")
        data = {"searchType":"searchByMatricule", 
                "HTML_FORM_FIELD":c}
        response = session.post(url, data = data, headers = headers)
        if response.status_code == 200:
            print(f"Captcha bypass attempted with HTTP response code {response.status_code}. Test scraper to verify pass.")
        else:
            print(f"Captcha bypass failed with response code {response.status_code}.")    