import pandas as pd
import matplotlib.pyplot as plt
from src.main.python.loader import load_data


FILE_NAME = 'meteo_data.json.gz'



data = load_data(FILE_NAME)

df = pd.DataFrame(data)

df['date'] =pd.to_datetime(df.date)


df_by_year = df.groupby(df['date'].map(lambda x: x.year)).mean()


print(df_by_year)
