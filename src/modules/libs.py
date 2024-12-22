def filtering(loan):
    import pandas as pd 
    filtered_loan = loan[loan.LoanPurpose.str.lower() == "car"]
    filtered_loan = filtered_loan[["UID", "LoanPurpose"]]
    return filtered_loan

def execute_query(query, db_params):
    import psycopg2
    """
    Connects to the PostgreSQL database and executes the given query.
    
    Parameters:
    - query: The SQL query to execute (string).
    - db_params: Dictionary containing the database connection parameters.
    
    Returns:
    - result: The result of the query (tuple or list of tuples).
    """
    try:
        # Establish the connection to the PostgreSQL server
        connection = psycopg2.connect(**db_params)
        print("Connection successful")

        # Create a cursor object
        with connection.cursor() as cursor:
            # Execute the query
            cursor.execute(query)
            
            # Fetch the result if the query is a SELECT
            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
            else:
                # For non-SELECT queries, we don't need to fetch anything
                result = None
                connection.commit()  # Commit the changes for INSERT, UPDATE, DELETE

            return result

    except Exception as e:
        print("Error:", e)
        return None

    finally:
        # Close the connection
        if connection:
            connection.close()
            print("Connection closed")


def send_dataframe_to_db(df, table_name, db_params):
    from dotenv import load_dotenv
    from sqlalchemy import create_engine 
    import os

    """
    Sends a Pandas DataFrame to a PostgreSQL database.

    Parameters:
    - df: The Pandas DataFrame to send.
    - table_name: The name of the table where the DataFrame should be inserted.
    """
   

    # Create the connection URL for SQLAlchemy
    connection_url = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    
    try:
        # Create a connection engine
        engine = create_engine(connection_url)
        
        # Send the DataFrame to the database
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"DataFrame has been successfully sent to the '{table_name}' table.")

    except Exception as e:
        print("Error:", e)
