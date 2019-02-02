import pandas as pd
from src.main.python.loader import load_data
import numpy as np

FILE_NAME = 'meteo_data.json.gz'

df_country_city = pd.read_csv('gps_country_city.csv')

data = load_data(FILE_NAME)

df_meteo = pd.DataFrame(data)

print(df_country_city)
print(df_meteo)


def closest_node(node, nodes):
    nodes = np.asarray(nodes)
    deltas = nodes - node
    dist_2 = np.einsum('ij,ij->i', deltas, deltas)
    return np.argmin(dist_2)


# new_df = pd.merge(df_country_city, df_meteo, how='left', left_on=['Latitude', 'Longitude'], right_on=['lat', 'long'])
# print(new_df)
A = df_country_city.assign(key=1)
B = df_meteo.assign(key=1)

merged_AB = pd.merge(A, B, on='key', suffixes=('_A', '_B'))

M = merged_AB.groupby('attr1_A').apply(lambda x: abs(x['Latitude'] - x['Longitude']) == abs(x['lat'] - x['long']).min())

merged_AB[M.values].drop_duplicates().drop('key', axis=1)

print(M)
