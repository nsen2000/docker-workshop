#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# df = pd.read_csv(
#     'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz',
#     dtype=dtype,
#     parse_dates=parse_dates
# )


# In[4]:


# df.head()


# # In[5]:


# df['tpep_pickup_datetime']


# # In[6]:


# get_ipython().system('uv add sqlalchemy')


# # # In[7]:


# get_ipython().system('uv add psycopg2-binary')






def run():
    
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'

    year = 2021
    month = 1
    
    target_table = 'yellow_taxi_data'
    
    chunksize = 100000
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
            )
            first = False
            
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append'
        )
        
if __name__ == '__main__':
    run()



# dtype = {
#     "VendorID": "Int64",
#     "passenger_count": "Int64",
#     "trip_distance": "float64",
#     "RatecodeID": "Int64",
#     "store_and_fwd_flag": "string",
#     "PULocationID": "Int64",
#     "DOLocationID": "Int64",
#     "payment_type": "Int64",
#     "fare_amount": "float64",
#     "extra": "float64",
#     "mta_tax": "float64",
#     "tip_amount": "float64",
#     "tolls_amount": "float64",
#     "improvement_surcharge": "float64",
#     "total_amount": "float64",
#     "congestion_surcharge": "float64"
# }

# parse_dates = [
#     "tpep_pickup_datetime",
#     "tpep_dropoff_datetime"
# ]

# df = pd.read_csv(
#     'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz',
#     dtype=dtype,
#     parse_dates=parse_dates
# )

# from sqlalchemy import create_engine
# engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# df_iter = pd.read_csv(
#     prefix + 'yellow_tripdata_2021-01.csv.gz',
#     dtype=dtype,
#     parse_dates=parse_dates,
#     iterator=True,
#     chunksize=100000
# )

# for df_chunk in df_iter:
#     print(len(df_chunk))
    
# df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# first = True

# for df_chunk in df_iter:

#     if first:
#         # Create table schema (no data)
#         df_chunk.head(0).to_sql(
#             name="yellow_taxi_data",
#             con=engine,
#             if_exists="replace"
#         )
#         first = False
#         print("Table created")

#     # Insert chunk
#     df_chunk.to_sql(
#         name="yellow_taxi_data",
#         con=engine,
#         if_exists="append"
#     )

#     print("Inserted:", len(df_chunk))