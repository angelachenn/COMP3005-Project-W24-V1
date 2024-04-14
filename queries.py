# Imports
import psycopg
import csv
import subprocess
import os
import re

# Connection Information
''' 
The following is the connection information for this project. These settings are used to connect this file to the autograder.
You must NOT change these settings - by default, db_host, db_port and db_username are as follows when first installing and utilizing psql.
For the user "postgres", you must MANUALLY set the password to 1234.

This can be done with the following snippet:

sudo -u postgres psql
\password postgres

'''
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

# Directory Path - Do NOT Modify
dir_path = os.path.dirname(os.path.realpath(__file__))

# Loading the Database after Drop - Do NOT Modify
#================================================
def load_database(conn):
    drop_database(conn)

    cursor = conn.cursor()
    # Create the Database if it DNE
    try:
        conn.autocommit = True
        cursor.execute(f"CREATE DATABASE {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)

    finally:
        cursor.close()
        conn.autocommit = False
    conn.close()
    
    # Connect to this query database.
    dbname = query_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    # Import the dbexport.sql database data into this database
    try:
        command = f'psql -h {host} -U {user} -d {query_database_name} -a -f "{os.path.join(dir_path, "dbexport.sql")}" > /dev/null 2>&1'
        env = {'PGPASSWORD': password}
        subprocess.run(command, shell=True, check=True, env=env)

    except Exception as error:
        print(f"An error occurred while loading the database: {error}")
    
    # Return this connection.
    return conn    

# Dropping the Database after Query n Execution - Do NOT Modify
#================================================
def drop_database(conn):
    # Drop database if it exists.

    cursor = conn.cursor()

    try:
        conn.autocommit = True
        cursor.execute(f"DROP DATABASE IF EXISTS {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)
        pass

    finally:
        cursor.close()
        conn.autocommit = False

# Reconnect to Root Database - Do NOT Modify
#================================================
def reconnect():
    dbname = root_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Getting the execution time of the query through EXPLAIN ANALYZE - Do NOT Modify
#================================================
def get_time(cursor, sql_query):
    # Prefix your query with EXPLAIN ANALYZE
    explain_query = f"EXPLAIN ANALYZE {sql_query}"

    try:
        # Execute the EXPLAIN ANALYZE query
        cursor.execute(explain_query)
        
        # Fetch all rows from the cursor
        explain_output = cursor.fetchall()
        
        # Convert the output tuples to a single string
        explain_text = "\n".join([row[0] for row in explain_output])
        
        # Use regular expression to find the execution time
        # Look for the pattern "Execution Time: <time> ms"
        match = re.search(r"Execution Time: ([\d.]+) ms", explain_text)
        if match:
            execution_time = float(match.group(1))
            return f"Execution Time: {execution_time} ms"
        else:
            print("Execution Time not found in EXPLAIN ANALYZE output.")
            return f"NA"
        
    except Exception as error:
        print(f"[ERROR] Error getting time.\n{error}")


# Write the results into some Q_n CSV. If the is an error with the query, it is a INC result - Do NOT Modify
#================================================
def write_csv(execution_time, cursor, i):
    # Collect all data into this csv, if there is an error from the query execution, the resulting time is INC.
    try:
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        filename = f"{dir_path}/Q_{i}.csv"

        with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Write column names to the CSV file
            csvwriter.writerow(colnames)
            
            # Write data rows to the CSV file
            csvwriter.writerows(rows)

    except Exception as error:
        execution_time[i-1] = "INC"
        print(error)
    
#================================================
        
'''
The following 10 methods, (Q_n(), where 1 < n < 10) will be where you are tasked to input your queries.
To reiterate, any modification outside of the query line will be flagged, and then marked as potential cheating.
Once you run this script, these 10 methods will run and print the times in order from top to bottom, Q1 to Q10 in the terminal window.
'''
def Q_1(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, AVG(s.xg) as avg_xg
            FROM players p
            JOIN events e ON p.id = e.player_id
            JOIN shots s ON e.id = s.event_id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' AND c.season_name = '2020/2021' AND s.xg > 0
            GROUP BY p.name
            HAVING COUNT(s.event_id) > 0
            ORDER BY avg_xg DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[0] = (time_val)

    write_csv(execution_time, cursor, 1)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_2(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, COUNT(s.event_id) AS total_shots
            FROM players p
            JOIN events e ON p.id = e.player_id
            JOIN shots s ON e.id = s.event_id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name = '2020/2021'
            GROUP BY p.name
            HAVING COUNT(s.event_id) >= 1
            ORDER BY total_shots DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[1] = (time_val)

    write_csv(execution_time, cursor, 2)

    cursor.close()
    new_conn.close()

    return reconnect()
    
def Q_3(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, COUNT(s.event_id) AS first_time_shots
            FROM players p
            JOIN events e ON p.id = e.player_id
            JOIN shots s ON e.id = s.event_id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name IN ('2020/2021', '2019/2020', '2018/2019')
            AND s.first_time = TRUE
            GROUP BY p.name
            HAVING COUNT(s.event_id) >= 1
            ORDER BY first_time_shots DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[2] = (time_val)

    write_csv(execution_time, cursor, 3)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_4(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT t.team_name, COUNT(pass.event_id) AS passes_made
            FROM teams t
            JOIN events e ON t.team_id = e.team_id
            JOIN passes pass ON pass.event_id = e.id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name = '2020/2021'
            GROUP BY t.team_name
            HAVING COUNT(pass.event_id) >= 1
            ORDER BY passes_made DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[3] = (time_val)

    write_csv(execution_time, cursor, 4)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_5(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, COUNT(e.player_id) AS num_passes_received
            FROM events e
            JOIN ball_receipts r ON e.id = r.event_id
            JOIN players p ON p.id = e.player_id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'Premier League' 
            AND c.season_name = '2003/2004'
            GROUP BY p.name
            HAVING COUNT(e.player_id) >= 1
            ORDER BY num_passes_received DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[4] = (time_val)

    write_csv(execution_time, cursor, 5)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_6(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:
    
    query = """
            SELECT t.team_name, COUNT(shot.event_id) AS shots_made
            FROM teams t
            JOIN events e ON t.team_id = e.team_id
            JOIN shots shot ON shot.event_id = e.id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'Premier League' 
            AND c.season_name = '2003/2004'
            GROUP BY t.team_name
            HAVING COUNT(shot.event_id) >= 1
            ORDER BY shots_made DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[5] = (time_val)

    write_csv(execution_time, cursor, 6)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_7(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, COUNT(pass.event_id) AS through_balls
            FROM passes pass
            JOIN events e ON pass.event_id = e.id
            JOIN players p ON e.player_id = p.id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name = '2020/2021'
            AND pass.through_ball = TRUE
            GROUP BY p.name
            HAVING COUNT(pass.event_id) >= 1
            ORDER BY through_balls DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[6] = (time_val)

    write_csv(execution_time, cursor, 7)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_8(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT t.team_name, COUNT(pass.event_id) AS through_balls
            FROM teams t
            JOIN events e ON t.team_id = e.team_id
            JOIN passes pass ON pass.event_id = e.id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name = '2020/2021'
            AND pass.through_ball = TRUE
            GROUP BY t.team_name
            HAVING COUNT(pass.event_id) >= 1
            ORDER BY through_balls DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[7] = (time_val)

    write_csv(execution_time, cursor, 8)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_9(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, COUNT(d.event_id) AS successful_dribbles
            FROM players p
            JOIN events e ON p.id = e.player_id
            JOIN dribbles d ON e.id = d.event_id
            JOIN outcomes o ON d.outcome_id = o.id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name IN ('2020/2021', '2019/2020', '2018/2019')
            AND o.name = 'Complete'
            GROUP BY p.name
            HAVING COUNT(d.event_id) >= 1
            ORDER BY successful_dribbles DESC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[8] = (time_val)

    write_csv(execution_time, cursor, 9)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_10(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """ 
            SELECT p.name, COUNT(e.id) AS dribbled_past
            FROM players p
            JOIN events e ON p.id = e.player_id
            JOIN types d ON e.type_id = d.id
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE c.competition_name = 'La Liga' 
            AND c.season_name = '2020/2021'
            AND d.name = 'Dribbled Past'
            GROUP BY p.name
            HAVING COUNT(e.id) >= 1
            ORDER BY dribbled_past ASC;
            """

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[9] = (time_val)

    write_csv(execution_time, cursor, 10)

    cursor.close()
    new_conn.close()

    return reconnect()

# Running the queries from the Q_n methods - Do NOT Modify
#=====================================================
def run_queries(conn):

    execution_time = [0,0,0,0,0,0,0,0,0,0]

    conn = Q_1(conn, execution_time)
    conn = Q_2(conn, execution_time)
    conn = Q_3(conn, execution_time)
    conn = Q_4(conn, execution_time)
    conn = Q_5(conn, execution_time)
    conn = Q_6(conn, execution_time)
    conn = Q_7(conn, execution_time)
    conn = Q_8(conn, execution_time)
    conn = Q_9(conn, execution_time)
    conn = Q_10(conn, execution_time)

    for i in range(10):
        print(execution_time[i])

''' MAIN '''
try:
    if __name__ == "__main__":

        dbname = root_database_name
        user = db_username
        password = db_password
        host = db_host
        port = db_port

        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        
        run_queries(conn)
except Exception as error:
    print(error)
    #print("[ERROR]: Failure to connect to database.")
#_______________________________________________________
