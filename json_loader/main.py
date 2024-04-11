import psycopg
from loader import Loader

DATA_PATH = "json_loader/data"

if __name__ == '__main__':
  
  # Create a connection to the database
  conn = psycopg.connect(
    dbname = "project_database", # Mandated by queries.py
    user = "postgres", 
    host= "localhost",
    password = "password",
    port = 5432
  )

  # Create a cursor to execute queries in the database
  cur = conn.cursor()

  loader = Loader(cur, conn, DATA_PATH)
  loader.load()
  print("Completed JSON loading.")
