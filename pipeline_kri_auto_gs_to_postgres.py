from datetime import timedelta
from prefect import task, Flow
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import pandas as pd
from sqlalchemy import create_engine

# Google Sheets credentials
credentials = {
    'type': 'service_account',
    'project_id': 'kri-auto-dashboard',
    'private_key_id': '40fc01f02a01a0af207dfe3da4780f151dce2ac2',
    'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCve3FWodZ9ckmV\nhOgNHKpIlGFUtgT5nLkDMKGHq3iTtYkvzEdHQ432l1W/EGjd7bmNJRvoMN4v5LU5\n6hi3ebwToA3Dr2KbvADvADeV7nIRkGHV5LEmNWXbZloKfOzrDIgU9+d/hN8wkE0C\nl/ddKJXwKE7i74iDdBzMwEeCrR2HDk0fRBGX7BTgwzi8tpyfO6kzmY/mwS10ZWqT\nHLgC5RqQUgx6N+Z1uYjDGQcPCbtBtN/37VCTHTVW1lgFerxPtTflBg9YpUycmyGT\nWY7Lm2ROUGc/VMeh6RBLnv11ToBZcaOz7hImcC1ot3RI5y8PRdDbF+OVRBIhDE0D\n01JuBEbTAgMBAAECggEASyVJASi3oaut0B4Egwzp7L/BuFVj5wJShgeuvFrU/Sfp\nkfLf+Sf4JIdk3DdqONMiuMsR2soGc6r0YlMZd4RlYmARVOGHkBofqjlFFGryJQxX\noRFYPPzz29LGLySVDgilQ5lvZH+hVfoNSFWMQ7PsFVYlhe++XVB9Pr7+QHrioKDs\ndKh+tvL26rRK5Fzw3k9dALVDbmHbLdvjl/kz+GWoJW1E+bgRDtFi/2Ctn08PW+K/\n4WmnifNYN/bIkYAAQh6kU8nj9hMIa1Z+YvB3v1edUlzSM5U/IVUunaQ82yPf95ZY\nW1KaMlhYxQQXowv5tfC21uqoeLvWO4Pp5kF6PtK6HQKBgQDnRNEwo6gOjF2Cy7eD\nm7EidyL6t87l8xpqVeMp1dqCmcVcnVi7so886anVTNV1moScqqlF51C0svWvHU5A\nUGg7bcJ3+H6TnISNiGBDXioSG5s/uXHyDxdi59rtWj2fsYTh3KDr7z8daageXJPI\nJsWK52PZAzpkkCSEUHzPKyndpwKBgQDCP2s8XDBzUgvo6tq8Zb/U3mKKcOrisD8+\nSYXehlj+1qa3j80HwPek9U8hju7mIAP2/KterFaNCwSsK4K19yo6VwjZUoFJ+G5j\nPwUS5F2LHlwsvdcGK16zlJsGFPmY/r5jBcE1/NyTLcdYOs33HcAh+pFHagWjJ6D9\neTJJCvRq9QKBgQCK3rbihxMuETlBhgRfQcke0f0uIdtaFx1ghsxOXbzFOYLadx1G\nMBV01TaG/4kaAjvpO01DzX+X0fJXQbiwQ9gi/2iL06pmBtFNj3uGWG/Yybzyie+T\nE17OpDzA07Q3RUhuu6Xhppr2lXA/MwYGZMmv+/vn3tlcc3WKAi6/08Ji9wKBgQCN\nuUe8WmbfPNWDsxa0rmgwH7E14Pz/OChsgagym0MDbAlnlHu7VIday8BYc7jKHkHG\nSsOd54+eiJN4KqbLrPIabrX+GbxLA/9GWgyRpBy8DAKkgj4IOkx2Kc6RuWwCvJqc\nFO7LPRqSJ6xyKzVrP0GXiQHGYQyL5bYIOgz+TgfWoQKBgBLnz0CKbMyfSVoAZqrz\nIVxGPL8oEa05BV87BeSWbSul+1ajFwksYN5VrD/lNGe7PzGJ/VB1z6Af4OjO81ZD\nhl+viw6vSAw75sEIUt+G1+6JCB/9dwu4ajhj6AZC5nHDop25bCVPBSpQ3py742GI\nKbvg+Z3+JOGzg87pbhyZ1s8i\n-----END PRIVATE KEY-----\n',
    'client_email': 'kri-auto@kri-auto-dashboard.iam.gserviceaccount.com',
    'client_id': '109436484526841420688',
    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
    'token_uri': 'https://oauth2.googleapis.com/token',
    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
    'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/kri-auto%40kri-auto-dashboard.iam.gserviceaccount.com',
    'universe_domain': 'googleapis.com'
}
creds = json.dumps(credentials)

# Define the scope and path to your JSON credentials file
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Create credentials and authorize the client
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials)
gc = gspread.authorize(credentials)

# Open the Google Sheets document by its URL
spreadsheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1Ftxob0a-paGitcTzslZ8CQlYk6EaNdDFNWLa2A82FsE/edit#gid=0")

# Select a specific worksheet by title or index (e.g., by title)
worksheet = spreadsheet.worksheet('Suivi')

# Get all values from the worksheet
data = worksheet.get_all_values()[1:]

# Create a Pandas DataFrame from the data
new_column_names = ["client", "canal", "ville", "aéroport", "date réservation", "date début", "date fin", "matricule", "modèle", "tarif", "km début", "km fin", "commentaire"]
df = pd.DataFrame(data, columns=new_column_names)

df_filtered = df[df['client'] != '']
df_filtered['date début'] = pd.to_datetime(df_filtered['date début'], format='%d/%m/%Y', errors='coerce')
df_filtered['date fin'] = pd.to_datetime(df_filtered['date fin'], format='%d/%m/%Y', errors='coerce')

# Initialize an empty list to store transformed data
transformed_data = []

# Iterate over the rows in the original DataFrame
for _, row in df_filtered.iterrows():
    # Calculate the date range between "date début" and "date fin"
    date_range = pd.date_range(start=row['date début'], end=row['date fin'] - timedelta(days=1))

    # Create a new DataFrame for each day in the date range
    for date in date_range:
        transformed_row = row.copy()
        transformed_row['date'] = date
        transformed_data.append(transformed_row)

# Create a new DataFrame from the transformed data
transformed_df = pd.DataFrame(transformed_data)

# Drop the original "date début" and "date fin" columns
transformed_df.drop(['date début', 'date fin'], axis=1, inplace=True)

# Reset the index
transformed_df.reset_index(drop=True, inplace=True)
transformed_df['date'] = transformed_df['date'].astype(str)

# Define PostgreSQL connection details
db_config = {
    'user': 'postgres',
    'password': 'mysecretpassword',
    'host': 'my_postgres',
    'port': '5432',
    'database': 'test_database'
}

# Create a connection string
connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Create an SQLAlchemy engine
engine = create_engine(connection_string)

# Define a task to load data into PostgreSQL
@task
def load_data_to_postgres(dataframe, table_name):
    dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data loaded to table {table_name}")

# Define the Prefect flow
with Flow("Google Sheets to PostgreSQL") as flow:
    load_data_to_postgres(transformed_df, "sales_data")

# Execute the flow
if __name__ == "__main__":
    flow.run()

