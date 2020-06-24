from bs4 import BeautifulSoup
from functools import reduce
from memory_optimizers import * 
from tqdm import tqdm

import itertools
import pandas as pd
import random
import re
import sqlite3
import sys

def load_data(filename, low, amount):
    sqlite_file = filename
    conn = sqlite3.connect(sqlite_file)
    high = low + amount
    all_rows = [item for item in 
                conn.execute('SELECT * FROM {tn} WHERE rowid >= {low} AND rowid <= {high}'.format(tn="html_pages", 
                                                                               low=str(low),
                                                                               high=str(high)))]
    conn.close()
    return(all_rows)

# Define intervals for SQL queries to be processed and passed to HTML parser.
# Necessary to avoid memory overload (cannot load entire SQL database at once).
def get_intervals(low, high, interval):
    intervals = [*range(low,high,interval)]
    return(intervals)

def remove_tabs_rows(df):
    df.replace('\s+',' ',regex=True,inplace=True)
    return(df)

# Create list of rows to be filled in parser.
def prep_rows(html):
    # Table 0 prep
    identification_rows = []

    # Table 1 prep
    idx = list(range(0,len(html)))
    properietare = pd.DataFrame(index = idx)
    properietare_rows = []
    headers = []

    # Tables 2 and 3 prep
    carasteriques_terrain_rows = []
    carasteriques_batiment_rows = []
    courant_anterieur_rows = []

    # Tables 4 and 5 prep
    reparation_rows = []

    # Tables 6 and 7 prep
    repartition_des_valeurs_ET_source_legislative_rows = []
    meta_rows = []

    # Tax years prep
    tax_years_list = []
    return(identification_rows,properietare_rows,headers,carasteriques_terrain_rows,carasteriques_batiment_rows,
          courant_anterieur_rows,reparation_rows,repartition_des_valeurs_ET_source_legislative_rows,meta_rows,
          tax_years_list)

def process_table_zero(table):
    table_rows = table.find_all('tr')
    l = []
    for tr in table_rows:
        th = tr.find_all('th')
        row = [tr.text for tr in th][0]
        l.append(row)
    return(l)

def process_table_one_alternative(table,headers,property_id):
    table_rows = table.find_all('tr')
    table_headers = []
    dct = {}
    for tr in table_rows:
        td = tr.find_all('td')
        th = tr.find_all('th')
        header = [re.sub('[^A-Za-z0-9 ]+', '', tr.text).replace(" ", "_") for tr in td][0]
        if header == "":
            continue
        data = [tr.text for tr in th][0]
        if header not in headers:
            dct[header] = data
            headers.append(header)
        elif header in headers:
            if header not in table_headers:
                dct[header] = data
            if header in table_headers:
                header_count = table_headers.count(header)
                new_header = header + str(header_count + 1)
                dct[new_header] = data
                headers.append(new_header)
        table_headers.append(header)
    dct['Numero_de_matricule'] = property_id
    return(dct)

def process_table_one(table,properietare,counter):
    table_rows = table.find_all('tr')
    table_headers = []
    for tr in table_rows:
        td = tr.find_all('td')
        th = tr.find_all('th')
        header = [re.sub('[^A-Za-z0-9 ]+', '', tr.text).replace(" ", "_") for tr in td][0]
        if header == "":
            continue
        data = [tr.text for tr in th][0]
        if header not in properietare.columns:
            properietare[header] = np.nan
            properietare[header][counter] = data
        elif header in properietare.columns:
            if header not in table_headers:
                properietare[header][counter] = data
            if header in table_headers:
                header_count = table_headers.count(header)
                new_header = header + str(header_count + 1)
                properietare[new_header] = np.nan
                properietare[new_header][counter] = data
        table_headers.append(header)
        
def process_table_two(table,property_id):
    tables_heads = table.find_all('h3')
    table_rows = table.find_all('tr')
    l_one = []
    l_two = []
    for tr in table_rows:
        th = tr.find_all('th')
        col_one_row = [tr.text for tr in th][0]
        col_two_row = [tr.text for tr in th][1]
        l_one.append(col_one_row)
        l_two.append(col_two_row)
    l_one = l_one[1:3]
    l_one.append(property_id)
    l_two = l_two[1:9]
    l_two.append(property_id)
    return(l_one, l_two)

def process_table_three(table,property_id):
    table_rows = table.find_all('tr')
    l = []
    for tr in table_rows:
        th = tr.find_all('th')
        row = [tr.text for tr in th]
        l.append(row)
    l = [item for sublist in l[1:5] for item in sublist]
    l.append(property_id)
    return(l)

def process_tables_four_and_five(tables,property_id):
    l = []
    for table in tables:
        table_rows = table.find_all('tr')
        for tr in table_rows:
            th = tr.find_all('th')
            row = [tr.text for tr in th]
            l.append(row)
    l = [item for sublist in l for item in sublist]
    l.append(property_id)
    return(l)

def process_table_seven(table,property_id):
    table_rows = table.find_all('tr')
    l = []
    for tr in table_rows:
        td = tr.find_all('td')
        try:
            row_td = [tr.text[-10:] for tr in td][0]
        except:
            pass
        l.append(row_td)
        th = tr.find_all('th')
        try:
            row_th = [tr.text[6:] for tr in th][0]
            l = l[0:2]
            l.append(row_th)
        except:
            pass
    if len(l) < 3:
        l.append("")
    l.append(property_id)
    return(l)

def process_table_six(table,property_id):
    table_rows = table.find_all('tr')
    l = []
    for tr in table_rows[2:]:
        td = tr.find_all('td')
        try:
            row_td = [' '.join(re.sub(r"[\n\t]*", "", tr.text).split()) 
                      for tr in td][0]
            l.append(row_td)
        except:
            pass
        th = tr.find_all('th')
        row_th = [tr.text.strip() for tr in th][0]
        l.append(row_th)
    l = pd.Series(l)
    even = [0]
    odd = []
    for num in range(1,len(l)): 
        if num % 2 == 0:
            even.append(num)
        else:
            odd.append(num)
    d = dict(zip(list(l[even]), list(l[odd])))
    d["Numero_de_matricule"] = property_id
    return(d)

# Error handling for HTML loop.
def error_handling(pages_processed,table_nr,exception_tables):
    print("--> EXCEPTION at page {0} in batch and table {1}.".format(pages_processed,table_nr))
    exception_tables.append(table_nr)
    return(exception_tables)

# Main loop for parsing HTML. Goes through each html, checks which tables are available, registers errors. 
# As the loop progresses, values from tables are appended as lists or dicts into separate lists for each table.
# Returns a set of lists of lists and dictionaries.
# Only data not processed is a list of tax years.
def loop_through_html(html,
                      identification_rows,
                      properietare_rows,
                      headers,
                      carasteriques_terrain_rows,
                      carasteriques_batiment_rows,
                      courant_anterieur_rows,
                      reparation_rows,
                      repartition_des_valeurs_ET_source_legislative_rows,
                      meta_rows,
                      tax_years_list):
    pages_processed = 0
    exceptions = []
    
    # Create random sample of numbers to save HTMLs for validation
    try:
        sample_nrs = random.sample(range(0, len(html)), 10)
    except:
        print("Nothing to sample.")
    for site in tqdm(html):
        # Save suspiciously large sites as errors.
        size = sys.getsizeof(site)
        #if size > 30000:
        #    with open(f"tailend_errors/{pages_processed}.html", "w") as f:
        #        f.write(site.decode("utf-8")) 
        
        soup = BeautifulSoup(site.decode(), features = "html.parser")
        tables = soup.findAll('table')
        exception_tables = []

        # TABLE ZERO: identification_rows
        # Fetch table and 'Numero_de_matricule'.
        # If this fails, pass entire site and append to error list.
        # Later 'Numero_de_matricule' used as key to merge tables.

        try:     
            table_zero = tables[0]
            identification_rows.append(process_table_zero(table_zero))
            property_id = process_table_zero(table_zero)[3]

            # TABLE ONE: properietare_rows
            try:    
                table_one = tables[1]
                properietare_rows.append(process_table_one_alternative(table_one,headers,property_id))
            except:
                exception_tables = error_handling(pages_processed,"one",exception_tables)

            # TABLES TWO: 1) carasteriques_terrain_rows 2) carasteriques_batiment_rows
            try:
                table_two = tables[2]
                terrain, batiment = process_table_two(table_two, property_id)
                carasteriques_terrain_rows.append(terrain)
                carasteriques_batiment_rows.append(batiment)
            except:
                exception_tables = error_handling(pages_processed,"two", exception_tables)

            # TABLE THREE: courant_anterieur_rows
            try:
                table_three = tables[3]
                courant_anterieur = process_table_three(table_three,property_id)
                courant_anterieur_rows.append(courant_anterieur)
            except:
                exception_tables = error_handling(pages_processed,"three",exception_tables)

            # TABLES FOUR AND FIVE: reparation_rows
            try:
                tables_four_five = tables[4:6]
                reparation_rows.append(process_tables_four_and_five(tables_four_five,property_id))
            except:
                exception_tables = error_handling(pages_processed,"four_five",exception_tables)

            # TABLES SIX AND SEVEN: 1) meta_rows 2) repartition_des_valeurs_ET_source_legislative_rows
            if len(tables) == 9:
                try:
                    table = tables[6]
                    meta_rows.append(process_table_seven(table,property_id))
                    repartition_des_valeurs_ET_source_legislative_rows.append(dict())
                except:
                    exception_tables = error_handling(pages_processed,"seven",exception_tables)
            if len(tables) == 10:
                try:
                    table = tables[7]
                    meta_rows.append(process_table_seven(table,property_id))
                    table = tables[6]
                    repartition_des_valeurs_ET_source_legislative_rows.append(process_table_six(table,property_id))
                except:
                    exception_tables = error_handling(pages_processed,"six_seven",exception_tables)
            
            # Save sampled pages as HTML.
            if(pages_processed in sample_nrs):
                with open(f"htmls/{property_id}.html", "w") as f: 
                    f.write(site.decode("utf-8"))
        # Prevent any pages that do not have a property ID to move forward in loop.
        except:
            exception_tables = error_handling(pages_processed,"zero",exception_tables)
        if len(exception_tables) > 0:
            exceptions.append((site, exception_tables))
        pages_processed += 1
    return(identification_rows,
           properietare_rows,
           carasteriques_terrain_rows,
           carasteriques_batiment_rows,
           courant_anterieur_rows,
           reparation_rows,
           repartition_des_valeurs_ET_source_legislative_rows,
           meta_rows,
           exceptions)

# Bring it all together: Turn lists and dictionaries from parsing into Pandas dataframes and merge.
def create_df(identification_rows,
              properietare_rows,
              carasteriques_terrain_rows,
              carasteriques_batiment_rows,
              courant_anterieur_rows,
              reparation_rows,
              repartition_des_valeurs_ET_source_legislative_rows,
              meta_rows):
    ## TABLE 0
    identification = pd.DataFrame(identification_rows, columns=["Adresse", "Arrondissement", "Numero_de_lot",
                                         "Numero_de_matricule", "Utilisation_predominante",
                                         "Numero_dunite_de_voisinage", "Numero_de_dossier"])
    identification.drop_duplicates(inplace=True)
    
    # TABLE 1
    properietare = pd.DataFrame(properietare_rows)
    properietare.Date_dinscription_au_rle = [re.sub('\s+',' ', value) 
                                             for value 
                                             in properietare.Date_dinscription_au_rle]
    properietare.drop_duplicates(inplace=True)

    ## TABLES 2
    ## Terrain
    carasteriques_terrain = pd.DataFrame(carasteriques_terrain_rows, columns=["Mesure_frontale", 
                                                                              "Superficie",
                                                                              "Numero_de_matricule"])
    carasteriques_terrain.drop_duplicates(inplace=True)
    
    # Batiment
    carasteriques_batiment = pd.DataFrame(carasteriques_batiment_rows, columns=["Nombre_detages", 
                                                                                "Annee_de_construction",
                                                                                "Aire_detages",
                                                                                "Genre_de_construction",
                                                                                "Lien_physique",
                                                                                "Nombre_de_logements",
                                                                                "Nombre_de_locaux_non_residentiels",
                                                                                "Nombre_de_chambres_locatives",
                                                                                "Numero_de_matricule"])
    carasteriques_batiment.drop_duplicates(inplace=True)
    # TABLE 3
    # Rôle courant & anterieur
    courant_anterieur_df = pd.DataFrame(courant_anterieur_rows, columns = [
        "Date_de_reference_au_marche_courant",
        "Date_de_reference_au_marche_anterieur",
        "Valeur_du_terrain_courant",
        "Valeur_de_limmeuble_au_role_anterieur",
        "Valeur_du_batiment_courant",
        "Valeur_de_limmeuble_courant",
        "Numero_de_matricule"])
    courant_anterieur_df.drop_duplicates(inplace=True)
    
    ## TABLES 4 & 5
    reparation = pd.DataFrame(reparation_rows, columns=[
        "Categorie_et_classe_dimmeuble_a_des_fins_dapplication_des_taux_varies_de_taxation",
        "Valeur_imposable_de_limmeuble",
        "Valeur_non_imposable_de_limmeuble",
        "Numero_de_matricule"])
    reparation.drop_duplicates(inplace=True)
    
    # TABLES 6 & 7
    repartition_et_source_df = pd.DataFrame.from_records(repartition_des_valeurs_ET_source_legislative_rows)
    repartition_et_source_df.columns = repartition_et_source_df.columns.str.replace(' ', '_')
    repartition_et_source_df.drop_duplicates(inplace=True)
    meta = pd.DataFrame(meta_rows, columns=["Informations_date_du","Date_du_rapport","Note","Numero_de_matricule"])
    meta.drop_duplicates(inplace=True)
    
    # MERGE TABLES
    # Check no df is empty.
    dfs = [df.set_index("Numero_de_matricule") 
           for df in [identification, properietare, 
                      carasteriques_terrain, carasteriques_batiment,
                      courant_anterieur_df,reparation,
                      repartition_et_source_df,meta] 
           if not df.empty]
 
    # Try to merge tables.
    try:
        full_data = reduce(lambda left,right: pd.merge(left,right,
                               left_index=True, right_index=True,
                               how='left'), dfs)
        
    # In case of error, run memory optimizing functions.
    except:
        print("There was an error. Trying to optimize memory use.")
        dfs_opt = []
        for df in dfs:
            df_opt = optimize_all(df)
            dfs_opt.append(df_opt)
        full_data = reduce(lambda left,right: pd.merge(left,right,
                                    left_index=True,right_index=True,
                                    how='left'),dfs_opt)

    full_data.drop_duplicates(inplace=True)
    full_data.replace('\s+',' ',regex=True,inplace=True)
    return(full_data)