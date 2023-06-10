import pandas as pd
def getMergedDataframe(model):
  # DATA
  data_col_names = ['ID', 'YEAR', 'ELEMENT']
  for i in range(1, 13):
      data_col_names.extend([f'VALUE{i}', f'DMFLAG{i}', f'QCFLAG{i}', f'DSFLAG{i}'])

  data_col_widths = [11, 4, 4] + [5, 1, 1, 1]*12

  data = pd.read_fwf(f'EXPLOREDATA/ghcnm.tavg.v4.0.1.20230609.{model}.dat', widths=data_col_widths, names=data_col_names)

  # METADATA
  metadata_col_names = ['ID', "LATITUDE", "LONGITUDE", "ELEVATION", "STATION"]
  metadata_col_widths = [12, 8, 10, 8, 25]
  metadata = pd.read_fwf(f'EXPLOREDATA/ghcnm.tavg.v4.0.1.20230609.{model}.inv', widths=metadata_col_widths, names=metadata_col_names)

  # MERGED DATA
  mergeddata = pd.merge(data, metadata, on='ID')
# Rename columnd ID to station ID
  mergeddata['Country Code'] = mergeddata['ID'].str[:2]
  mergeddata.rename(columns={'ID': 'STATION ID'}, inplace=True)

  # COUNTRY CODES
  countryCodes = pd.read_csv('COUNTRYDATA/COUNTRIES.csv')
  countryCodes.head()

  # Merge the dataframes and drop the ID column
  df = pd.merge(mergeddata, countryCodes, on='Country Code')
  # Rename Country column to COUNTRY
  df.rename(columns={'Country': 'COUNTRY'}, inplace=True)
  return df


def getLongTable(df):
  # Create a long table with the year, month, and value for each country and station
  df_long = pd.melt(df, id_vars=['STATION ID', 'COUNTRY', 'STATION', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'YEAR'], value_vars=[f'VALUE{i}' for i in range(1, 13)], var_name='MONTH', value_name='TEMP')
  # Divive TEMP by 100 to get the temperature in degrees Celsius
  df_long['TEMP'] = df_long['TEMP'] / 100
  # On month column, remove the string 'VALUE' and convert to integer
  df_long['MONTH'] = df_long['MONTH'].str[5:].astype(int)
  # Reorder Columns
  df_long = df_long[['STATION ID', 'COUNTRY', 'STATION', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'YEAR', 'MONTH', 'TEMP']]

  return df_long


def writeCSV(df, country = None):
  if (country):
    df = df[df['COUNTRY'] == country]
    df.to_csv(f'DATA/{country}.csv', index=False)
  else:
    for country in df['COUNTRY'].unique():
      df[df['COUNTRY'] == country].to_csv(f'DATA/{country}.csv', index=False)


def writeJSON(df, country = None):
  if (country):
    df = df[df['COUNTRY'] == country]
    df.to_json(f'JSONDATA/{country}.json', orient='records')
  else:
    for country in df['COUNTRY'].unique():
      df[df['COUNTRY'] == country].to_json(f'JSONDATA/{country}.json', orient='records')

df = getMergedDataframe('qfe')
df_long = getLongTable(df)
# writeCSV(df_long)
writeJSON(df_long)