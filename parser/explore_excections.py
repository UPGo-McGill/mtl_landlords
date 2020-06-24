import glob
import os
import pickle
import sys

from memory_optimizers import * 

indir = sys.argv[1] # Name of directory to read files from
outfile = sys.argv[2] # Name of directory and file to save merged CSVs
pklfiles = glob.glob(os.path.join(indir, '*.pkl')) # Create list of paths for csv files in indir

dataframes = []  # A list to hold all the individual pandas DataFrames
counter = 0

print(pklfiles)

for pklfile in pklfiles: # Loop through the csv files
    content = pickle.load(open(pklfile, 'rb'))
    print(pklfile)
    for c in content:
        #with open (f'{outfile}{counter}.html', 'wb') as f:
        #    f.write(c[0])
        #    f.close()
        counter += 1
        print(c[1])
        print(counter)