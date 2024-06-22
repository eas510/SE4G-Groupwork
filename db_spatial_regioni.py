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

try:
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host)
    cur = conn.cursor()

    # delete table if already exists
    cur.execute("DROP TABLE IF EXISTS emilia_romagna_boundary CASCADE;")
    conn.commit()

    # create table for Emilia-Romagna boundary
    create_table_query = """
    CREATE TABLE emilia_romagna_boundary (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        geom GEOMETRY(MultiPolygon, 4326)
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # retrive Emilia-Romagna boundary data
    gdf = ox.geocode_to_gdf('Emilia-Romagna, Italy')
    geometry = gdf.iloc[0].geometry

    # convert Polygon to MultiPolygon
    if isinstance(geometry, Polygon):
        geometry = MultiPolygon([geometry])

    # insert boundary data into table
    name = 'Emilia-Romagna'
    wkt_geom = geometry.wkt  # use Shapely wkt arrtibute

    insert_query = """
    INSERT INTO emilia_romagna_boundary (name, geom)
    VALUES (%s, ST_SetSRID(ST_GeomFromText(%s), 4326));
    """
    cur.execute(insert_query, (name, wkt_geom))

    # close connection
    conn.commit()
    cur.close()
    conn.close()

    print("Emilia-Romagna boundary has been inserted into the database.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()