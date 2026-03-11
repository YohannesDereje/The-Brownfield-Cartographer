import pandas as pd
df = pd.read_csv('data/raw_users.csv')
df.to_sql('stg_users', con='engine')