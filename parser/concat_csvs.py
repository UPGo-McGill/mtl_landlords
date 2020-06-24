import glob
import os
import pandas as pd
import sys

from memory_optimizers import * 

indir = sys.argv[1] # Name of directory to read files from
outfile = sys.argv[2] # Name of directory and file to save merged CSVs
csvfiles = glob.glob(os.path.join(indir, '*.csv')) # Create list of paths for csv files in indir

dataframes = []  # A list to hold all the individual pandas DataFrames
counter = 0

for csvfile in csvfiles: # Loop through the csv files
    print(csvfile)
    df = pd.read_csv(csvfile,low_memory=False)
    df.Date_du_raport = pd.to_datetime(df.Date_du_rapport)
    print(mem_usage(df))
    counter += 1
    df_opt = optimize_all(df)
    print(mem_usage(df_opt))
    dataframes.append(df_opt)
    print(f"Dataframe size was {df_opt.shape} files.")
    print(f"Processed {counter} files.")

print("Processed {0} files in total.\nProceeding to concatenate dataframes.".format(counter))

full_data = pd.concat(dataframes,ignore_index=True,sort=True) # Concatenate all dfs together
print("The shape of the merged dataframe is {}, with {} ids, out of which {} are unique.".format(
    full_data.shape, 
    len(full_data.Numero_de_matricule), 
    len(full_data.Numero_de_matricule.unique())))

full_data = full_data.sort_values('Date_du_rapport', ascending=True).drop_duplicates('Numero_de_matricule', keep='last')
print("The shape of the merged dataframe without duplicate rows is {}, with {} ids, out of which {} are unique.".format(
    full_data.shape, 
    len(full_data.Numero_de_matricule), 
    len(full_data.Numero_de_matricule.unique())))

with open(f"{outfile}.txt", "w") as f: 
    f.write(f"""The shape of the merged dataframe without duplicate rows is {full_data.shape}, with {len(full_data.Numero_de_matricule)} unique ids.
    With duplicates removed, the dataset contains {len(full_data.Numero_de_matricule.unique())} unique records.
    """)   
full_data.set_index("Numero_de_matricule",inplace=True) 
full_data.to_csv("{}.csv".format(outfile), encoding = "utf-8")

print("CSV with merged files saved.")
    