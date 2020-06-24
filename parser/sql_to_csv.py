from bs4 import BeautifulSoup
from tqdm import tqdm

import io
import os
import pandas as pd
import re
import numpy as np
import pickle
import time

from helpers import * 

# Load data from SQL.
timestamp = '20200509-160404'
sql_folder = f'../scraper/sql/{timestamp}'
# Set parameters for batch intervals.
lower = 0
upper = 10000
freq = 10000

low = 0
high = low + freq

for file in os.listdir(sql_folder):
    if file.endswith('.sqlite'):

        # Generate intervals for batches.
        intervals = get_intervals(lower,upper,freq)

        print("\nSTARTING JOB.")
        print("The lower bounds for the intervals of this job are " + str(intervals) + ".\n")

        # Create global variables for parsing metrics.
        total_files_processed = 0
        total_files_saved = 0
        total_errors = 0
        total_files_dropped = 0
        total_non_errors_dropped = 0

        for i,ival in tqdm(enumerate(intervals)):
            # Print interval for batch.
            print("\nSTARTING BATCH {} OUT OF {}.".format(str(i+1), len(intervals)))

            # Load batch from SQL.
            print("Loading data from SQL.")
            start_time = time.time()
            all_rows = load_data(f'{sql_folder}/{file}', ival, freq)
            print("--- %s seconds ---" % (time.time() - start_time))

            # Extract HTML from batch.
            html = [data[1] for data in all_rows]
            if len(html) == 0:
                break

            # Create lists for storing values parsed from HTML.
            print("Creating lists for storing values from HTML.")
            identification_rows,\
            properietare_rows,\
            headers,\
            carasteriques_terrain_rows,\
            carasteriques_batiment_rows,\
            courant_anterieur_rows,\
            reparation_rows,\
            repartition_des_valeurs_ET_source_legislative_rows,\
            meta_rows,\
            tax_years_list = prep_rows(html)

            # Update lists by looping through HTML.
            print("Parsing HTML for tables and appending lists.")
            identification_rows,\
            properietare_rows,\
            carasteriques_terrain_rows,\
            carasteriques_batiment_rows,\
            courant_anterieur_rows,\
            reparation_rows,\
            repartition_des_valeurs_ET_source_legislative_rows,\
            meta_rows,\
            exceptions = loop_through_html(
                html,
                identification_rows,
                properietare_rows,
                headers,
                carasteriques_terrain_rows,
                carasteriques_batiment_rows, 
                courant_anterieur_rows,
                reparation_rows,
                repartition_des_valeurs_ET_source_legislative_rows,
                meta_rows,
                tax_years_list)

            # Turn lists into dataframes and merge to get full data.
            print("\nCreate dataframes and merge data.")
            full_data = create_df(identification_rows,
                      properietare_rows,
                      carasteriques_terrain_rows,
                      carasteriques_batiment_rows,
                      courant_anterieur_rows,
                      reparation_rows,
                      repartition_des_valeurs_ET_source_legislative_rows,
                      meta_rows)

            # Save batch to csv.
            save_name = file.split(".")[0]
            full_data.to_csv(f"{save_name}.csv")
            print("Batch processed and saved.")

            # Try to save batch errors as pickle.
            try:    
                with open(f'exceptions{save_name}.pkl', 'wb') as f:
                    pickle.dump(exceptions, f)
                print("Batch errors processed and saved.")
            except:
                print("No errors for this batch.")

            # Create local variables for parsing metrics
            nr_files_processed = len(html)
            nr_files_saved = len(full_data.index)
            nr_errors = len(exceptions)
            nr_files_dropped = nr_files_processed - nr_files_saved
            nr_non_errors_dropped = nr_files_dropped + nr_errors

            # Message to output after parsing one batch.
            print("\nBATCH {} OUT OF {} FINISHED:".format(str(i+1), len(intervals)))
            print(("Started with {} HTML files. " +
                   "Dropped {} files, of which {} were errors. " + 
                   "Ended with {} files.\n").format(nr_files_processed, nr_files_dropped, nr_errors, nr_files_saved))

            # Update global variables for parsing metrics
            total_files_processed += nr_files_processed
            total_files_saved += nr_files_saved
            total_errors += nr_errors
            total_files_dropped += nr_files_dropped
            total_non_errors_dropped += nr_non_errors_dropped

# Message to output after parsing all data.  
print("ALL BATCHES PROCESSED AND SAVED INTO CSV-FILES.\n"+
      "SEE PICKLED EXCEPTION FILES FOR FAILED PAGES.\n")
print(("STARTED WITH A TOTAL OF {} HTML FILES.\n" + 
       "DROPPED A TOTAL OF {} FILES, OF WHICH {} WERE ERRORS.\n" +
       "SO A TOTAL OF {} DUPLICATES WERE DROPPED.\n" +
       "ENDED WITH {} FILES SAVED.\n").format(total_files_processed, 
                                           total_files_dropped, 
                                           total_errors, 
                                           total_non_errors_dropped, 
                                           total_files_saved))