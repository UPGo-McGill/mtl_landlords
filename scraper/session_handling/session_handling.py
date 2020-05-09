import requests
from fake_useragent import UserAgent

def initialize_session(baseURL = "https://servicesenligne2.ville.montreal.qc.ca/sel/evalweb/",
                       ua_browser = UserAgent().chrome):
    headers = {'User-Agent': str(ua_browser)}
    session = requests.Session()
    response = session.get(baseURL + "index", headers = headers)
    cookies = session.cookies.get_dict()
    if response.status_code == 200:
        print(f"Session successfully initialized with HTTP response code {response.status_code}.\nSession ID is '{cookies['JSESSIONID']}'.")
    else:
        print(f"Failed to initialize session with HTTP response code {img_response.status_code}.")
    return session, response, headers, cookies

