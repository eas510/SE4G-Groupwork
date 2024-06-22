import psycopg2
import osmnx as ox
import geopandas as gpd
from shapely import wkb
from shapely.geometry import Polygon, MultiPolygon

# Database connection parameters
db_name = "se4g"
user = "postgres"
password = "551010"
host = "localhost"


def create_table(cur):
    # delete table if already exists
    cur.execute("DROP TABLE IF EXISTS comuni CASCADE;")
    cur.execute("""
    CREATE TABLE comuni (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        geom GEOMETRY(MultiPolygon, 4326)
    );
    """)
    conn.commit()


def insert_comuni_data(cur, comuni):
    for idx, row in comuni.iterrows():
        name = row.get('name', 'Unknown')
        geom = row['geometry']

        # print(f"Geometry type before conversion: {type(geom)}")

        # convert Polygon to MultiPolygon
        if isinstance(geom, Polygon):
            geom = MultiPolygon([geom])

        # print(f"Geometry type after conversion: {type(geom)}")

        # only insert Polygon or MultiPolygon type of geometry data
        if isinstance(geom, (Polygon, MultiPolygon)):
            wkt_geom = geom.wkt
            cur.execute("""
                INSERT INTO comuni (name, geom)
                VALUES (%s, ST_SetSRID(ST_GeomFromText(%s), 4326));
            """, (name, wkt_geom))
    conn.commit()


try:
    # connect to database
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host)
    cur = conn.cursor()

    # create table
    create_table(cur)
    print("Comuni table created successfully.")

    # get Emilia-Romagna boundary data
    gdf = ox.geocode_to_gdf('Emilia-Romagna, Italy')
    polygon = gdf.iloc[0].geometry

    # get comuni boundary with new function name
    tags = {'boundary': 'administrative', 'admin_level': '8'}
    comuni = ox.features_from_polygon(polygon, tags)

    # insert data
    insert_comuni_data(cur, comuni)

    print("All comuni boundaries have been inserted into the database.")
    cur.close()
    conn.close()

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()