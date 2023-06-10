# Read and concat all files in the COUNTRIES folder and create a CSV file with all the data
import pandas as pd
import os

# Read all files in the COUNTRIES folder and concat them
df = pd.concat([pd.read_csv(f'COUNTRYDATA/{i}') for i in os.listdir('COUNTRYDATA')])
# Save the data in a CSV file
df.columns = ["Country Code", "Country"]
df.to_csv('COUNTRYDATA/COUNTRIES.csv', index=False)