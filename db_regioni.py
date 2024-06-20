import psycopg2
from datetime import datetime, timedelta
# import random
import requests
import json

# Database connection parameters
db_name = "se4g"
user = "postgres"
password = "551010"
host = "localhost"
# Connect to the default database to create the new database
conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host)
conn.autocommit = True # needed to create a db programmatically
cursor = conn.cursor()

# Create the database

try:
    cursor.execute(f"CREATE DATABASE {db_name};")
except: pass

conn.close()

# API URL
api_url = 'https://test.idrogeo.isprambiente.it/api/pir/regioni/8'

# request data
response = requests.get(api_url)
data = response.json()

# connect to database
conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host)
cursor = conn.cursor()

# define columns
fields = [
    ('uid', 'INT'),
    ('osmid', 'INT'),
    ('breadcrumb', 'JSONB'),
    ('nome', 'VARCHAR(50)'),
    ('extent', 'JSONB'),
    ('ar_kmq', 'FLOAT'),
    ('ar_id_p3', 'FLOAT'),
    ('ar_id_p2', 'FLOAT'),
    ('ar_id_p1', 'FLOAT'),
    ('aridp3_p', 'FLOAT'),
    ('aridp2_p', 'FLOAT'),
    ('aridp1_p', 'FLOAT'),
    ('pop_res011', 'INT'),
    ('pop_gio', 'INT'),
    ('pop_gio_p', 'FLOAT'),
    ('pop_adu', 'INT'),
    ('pop_adu_p', 'FLOAT'),
    ('pop_anz', 'INT'),
    ('pop_anz_p', 'FLOAT'),
    ('pop_idr_p3', 'INT'),
    ('pop_idr_p2', 'INT'),
    ('pop_idr_p1', 'INT'),
    ('popidp3_p', 'FLOAT'),
    ('popidp2_p', 'FLOAT'),
    ('popidp1_p', 'FLOAT'),
    ('fam_tot', 'INT'),
    ('fam_idr_p3', 'INT'),
    ('fam_idr_p2', 'INT'),
    ('fam_idr_p1', 'INT'),
    ('famidp3_p', 'FLOAT'),
    ('famidp2_p', 'FLOAT'),
    ('famidp1_p', 'FLOAT'),
    ('ed_tot', 'INT'),
    ('ed_idr_p3', 'INT'),
    ('ed_idr_p2', 'INT'),
    ('ed_idr_p1', 'INT'),
    ('edidp3_p', 'FLOAT'),
    ('edidp2_p', 'FLOAT'),
    ('edidp1_p', 'FLOAT'),
    ('im_tot', 'INT'),
    ('im_idr_p3', 'INT'),
    ('im_idr_p2', 'INT'),
    ('im_idr_p1', 'INT'),
    ('imidp3_p', 'FLOAT'),
    ('imidp2_p', 'FLOAT'),
    ('imidp1_p', 'FLOAT'),
    ('n_vir', 'INT'),
    ('bbcc_id_p3', 'INT'),
    ('bbcc_id_p2', 'INT'),
    ('bbcc_id_p1', 'INT'),
    ('bbccidp3_p', 'FLOAT'),
    ('bbccidp2_p', 'FLOAT'),
    ('bbccidp1_p', 'FLOAT'),
    ('ar_fr_p4', 'FLOAT'),
    ('ar_fr_p3', 'FLOAT'),
    ('ar_fr_p2', 'FLOAT'),
    ('ar_fr_p1', 'FLOAT'),
    ('ar_fr_aa', 'FLOAT'),
    ('ar_fr_p3p4', 'FLOAT'),
    ('ar_frp4_p', 'FLOAT'),
    ('ar_frp3_p', 'FLOAT'),
    ('ar_frp2_p', 'FLOAT'),
    ('ar_frp1_p', 'FLOAT'),
    ('ar_fraa_p', 'FLOAT'),
    ('ar_frp3p4p', 'FLOAT'),
    ('pop_fr_p4', 'INT'),
    ('pop_fr_p3', 'INT'),
    ('pop_fr_p2', 'INT'),
    ('pop_fr_p1', 'INT'),
    ('pop_fr_aa', 'INT'),
    ('popfr_p3p4', 'INT'),
    ('popfrp4_p', 'FLOAT'),
    ('popfrp3_p', 'FLOAT'),
    ('popfrp2_p', 'FLOAT'),
    ('popfrp1_p', 'FLOAT'),
    ('popfraa_p', 'FLOAT'),
    ('popfrp3p4p', 'FLOAT'),
    ('fam_fr_p4', 'INT'),
    ('fam_fr_p3', 'INT'),
    ('fam_fr_p2', 'INT'),
    ('fam_fr_p1', 'INT'),
    ('fam_fr_aa', 'INT'),
    ('famfr_p3p4', 'INT'),
    ('famfrp4_p', 'FLOAT'),
    ('famfrp3_p', 'FLOAT'),
    ('famfrp2_p', 'FLOAT'),
    ('famfrp1_p', 'FLOAT'),
    ('famfraa_p', 'FLOAT'),
    ('famfrp3p4p', 'FLOAT'),
    ('ed_fr_p4', 'INT'),
    ('ed_fr_p3', 'INT'),
    ('ed_fr_p2', 'INT'),
    ('ed_fr_p1', 'INT'),
    ('ed_fr_aa', 'INT'),
    ('ed_fr_p3p4', 'INT'),
    ('edfrp4_p', 'FLOAT'),
    ('edfrp3_p', 'FLOAT'),
    ('edfrp2_p', 'FLOAT'),
    ('edfrp1_p', 'FLOAT'),
    ('edfraa_p', 'FLOAT'),
    ('edfrp3p4p', 'FLOAT'),
    ('im_fr_p4', 'INT'),
    ('im_fr_p3', 'INT'),
    ('im_fr_p2', 'INT'),
    ('im_fr_p1', 'INT'),
    ('im_fr_aa', 'INT'),
    ('imfr_p3p4', 'INT'),
    ('imfrp4_p', 'FLOAT'),
    ('imfrp3_p', 'FLOAT'),
    ('imfrp2_p', 'FLOAT'),
    ('imfrp1_p', 'FLOAT'),
    ('imfraa_p', 'FLOAT'),
    ('imfrp3p4p', 'FLOAT'),
    ('bbcc_fr_p4', 'INT'),
    ('bbcc_fr_p3', 'INT'),
    ('bbcc_fr_p2', 'INT'),
    ('bbcc_fr_p1', 'INT'),
    ('bbcc_fr_aa', 'INT'),
    ('bbccfrp3p4', 'INT'),
    ('bbccfrp4_p', 'FLOAT'),
    ('bbccfrp3_p', 'FLOAT'),
    ('bbccfrp2_p', 'FLOAT'),
    ('bbccfrp1_p', 'FLOAT'),
    ('bbccfraa_p', 'FLOAT'),
    ('bbccfrp34p', 'FLOAT'),
    ('cod_rip', 'INT'),
    ('cod_reg', 'INT')
]

create_table_query = "CREATE TABLE IF NOT EXISTS api_data ("
for field_name, field_type in fields:
    create_table_query += f"{field_name} {field_type}, "
create_table_query = create_table_query.rstrip(", ") + ");"

cursor.execute(create_table_query)
conn.commit()

# Json to string
data['breadcrumb'] = json.dumps(data['breadcrumb'])
data['extent'] = json.dumps(data['extent'])

# filter empty key
filtered_data = {key: value for key, value in data.items() if value is not None}

insert_data_query = "INSERT INTO api_data ("
insert_data_query += ", ".join(filtered_data.keys())
insert_data_query += ") VALUES ("
insert_data_query += ", ".join([f"%({key})s" for key in filtered_data.keys()])
insert_data_query += ");"

cursor.execute(insert_data_query, filtered_data)
conn.commit()

cursor.close()
conn.close()