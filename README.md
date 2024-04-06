# COMP3005 Queries Code (ver 1.1)
> The following queries.py file is the template code for COMP3005 Database Project 1, which will be used for the autograder.
>
> **This README is an _important_ guide to effectively using this repository and finishing this project.**

### Updates (04/03/2024)
```Ver 1.0 -> Ver 1.1
- Added some more comments for accurate guidance
- Changed time collection - no longer using the overhead of Python but the SQL "EXPLAIN ANALYZE" computations.
```
## Starter Code:
- _queries.py_
  - This will be the file you will write your queries in **AND** submit.
- _dbexport.sql_
  - This is an **example** of an exported database. This can be used for testing, for both you and myself.

## Modules and Dependencies:
This project, and in turn the autograder and starter code, uses psycopg3 on a v22.04 Ubuntu Linux environment. 
- [psycopg3 Installation and Documentation](https://www.psycopg.org/psycopg3/docs/)
- [Carleton University VM Downloads](https://carleton.ca/scs/tech-support/virtual-machines/)

## Task:
As per the project guidelines,
>"Design a database that stores a soccer events dataset spanning multiple competitions and seasons. The provided
dataset is in JSON format and can be downloaded from https://github.com/statsbomb/open-data/tree/0067cae166a56aa80b2ef18f61e16158d6a7359a1. The documentation of the dataset is also available in the
above URL. After designing the database, you need to import the data from the JSON files into your database."

Once your database has been designed, you are then tasked to export this database into an .sql file named "dbexport" - this can be accomplished using [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html).

Given that the previous two steps are accomplished, your task is to now ONLY write your SQL queries within the prompted space within each Q_# method (where # is the question number).

## Testing:
The _queries.py_ file and your exported _dbexport.sql_ file should be within the same directory.

The _queries.py_ file includes the following code snippet,
```
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'
```
To briefly explain, these variables are used to connect to the root database named "project_database" and your query execution database named "query_database."
The code's process for each query is to create a database named "query_database", import your dbexport.sql file into this database, execute the query, and then drop the database (to avoid any alterations so results are not affected down the line).
The reasoning for two databases is because the connection cannot drop a database it is currently connected to, hence the two databases - one for a root connection and one for query execution.

You _may_ change these values to test on your end, **but under _no circumstance in the final deliverable should these initial values be different_.**

Expected Output:
> While testing, your output will be the dbexport.sql's log, and then following are your query times.
> **INC** simply means incomplete.

## Warning:
As the autograder is also connecting to your databases, to reiterate, any change to the initial values of the connection variables will result in your code submission becoming _void_. You may change these values for your own testing purposes, (e.g, you have a different password), but do so at your discretion.
  - What else will _VOID_ your submission?
    - Any _additional_ submissions in your repository.
    - _Other_ print statements.
    - Other alterations to code other than the query executions (please view ACADEMIC INTEGRITY below).
      
## Final Deliverable:
Your source code file(s) that maps and loads the existing JSON dataset from the JSON files into your database. This code must be stored in a directory named "json_loader".
Therefore, in your submission repository, you are _only_ submitting the script "queries.py", *your* dbexport.sql, and the "json_loader" directory.
Any additional submissions will ***void*** the entire ***code submission.***

## Bugs and Questions:
If you run into any fatal errors or bugs, please consult the [closed issues](https://github.com/gabrielmartell/COMP3005-Project-Template/issues?q=is%3Aissue+is%3Aclosed) first as it might have already been solved.
If it hasn't been solved, and/or you also have questions, please feel free to create an [open issue!](https://github.com/gabrielmartell/COMP3305-Project-Template/issues). If need be, you can also shoot me an email at gabemartell@cmail.carleton.ca.

# CARLETON ACADEMIC INTEGRITY
Any alterations to the code, such as modifying the time, will be flagged for suspicion of cheating - and thus will be reviewed by the staff and, if need be, the Dean.
To review the Integrity Violation Attributes of Carleton University, please view https://carleton.ca/registrar/academic-integrity/ 
