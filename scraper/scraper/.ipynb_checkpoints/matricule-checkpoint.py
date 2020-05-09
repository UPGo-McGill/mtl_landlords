import pandas as pd
import requests

def download_matricule_numbers(filename = "data/uniteevaluationfonciere.csv",
                               csv_url = 'http://donnees.ville.montreal.qc.ca/dataset/4ad6baea-4d2c-460f-a8bf-5d000db498f7/resource/2b9dfc3d-91d3-48de-b32c-a2a6d9417079/download/uniteevaluationfonciere.csv'):
    csv_response = requests.get(csv_url)
    if csv_response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(csv_response.content)
        print(f"CSV successfully downloaded and saved as '{filename}'")
    else:
        print(f"Failed to download CSV. HTTP response code for request was {csv_response.status_code}.")

def load_matricule_numbers(filename = "data/uniteevaluationfonciere.csv",
                           id_col = "MATRICULE83"):
    csv_file = pd.read_csv(filename)
    print(f"CSV file successfully loaded.\nThe total number of rows is {len(csv_file.index)}.\nThe total number of unique properties is {len(csv_file[id_col])}.")
    return csv_file

def prepare_matricule(ids = []):
    ids_to_scrape = []
    ids_split = [i.split("-") for i in ids]
    for row in ids_split:
        data = {"matriculeDiv":row[0],"matriculeSect":row[1],"matriculeEmpl":row[2],
            "matriculeCav":row[3],"matriculeBati":row[4],"matriculeCad":row[5]}
        ids_to_scrape.append(("-".join(row), data))
    print(f"Prepared a total of {len(ids_to_scrape)} rows for scraping.")
    return ids_to_scrape