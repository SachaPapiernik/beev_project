import os
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, create_engine
from dotenv import load_dotenv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def load_env() -> tuple:
    """Load environment variables for database connection.

    Returns:
        tuple: A tuple containing the database host, port, name, user, and password.

    Raises:
        EnvironmentError: If any of the required environment variables are not set.
    """

    # Load environment variables from .env file
    load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if any(v is None for v in [db_host, db_port, db_name, db_user, db_password]):
        raise EnvironmentError("Missing one or more required environment variables for database connection.")

    return db_host, db_port, db_name, db_user, db_password

def create_table(engine) -> None:
    """
    Creates two tables, 'car_data' and 'consumer_data', in a PostgreSQL database.

    The function establishes a connection to the database using the credentials loaded from the environment
    and defines the structure of the 'car_data' and 'consumer_data' tables with specific columns.

    Tables:
    - 'car_data':
        - Id (Integer, primary key)
        - Make (String)
        - Model (String)
        - Year (Integer)
        - Price (Integer)
        - Engine_Type (String)

    - 'consumer_data':
        - Id (Integer, primary key)
        - Country (String)
        - Make (String)
        - Model (String)
        - Year (Integer)
        - Review_Score (Float)
        - Sales_Volume (Integer)

    If the tables already exist in the database, the function prints a message indicating that the tables
    already exist. If an error occurs during the table creation process, it prints an error message.

    Returns:
    None
    """

    metadata = MetaData()

    # Define the car_data table
    car_data_table = Table('car_data', metadata,
                        Column('Id', Integer, primary_key=True),
                        Column('Make', String),
                        Column('Model', String),
                        Column('Year', Integer),
                        Column('Price', Integer),
                        Column('Engine_Type', String),
                        )

    # Define the consumer_data table
    consumer_data_table = Table('consumer_data', metadata,
                                Column('Id', Integer, primary_key=True),
                                Column('Country', String),
                                Column('Make', String),
                                Column('Model', String),
                                Column('Year', Integer),
                                Column('Review_Score', Float),
                                Column('Sales_Volume', Integer),
                                )

    try:

        # Create both table via the create method of the table object 
        consumer_data_table.create(engine)
        car_data_table.create(engine)
    except Exception as e:
        if "already exists" in str(e):
            print("Table already exists.")
        else:
            print(f"Error: {e}")
    else:
        print('Tables created')

def build_engine() -> None:
    """Builds and returns a SQLAlchemy engine for connecting to a PostgreSQL database.

    This function retrieves database connection details from the environment using the `load_env` function,
    and then creates a SQLAlchemy engine to connect to a PostgreSQL database.

    Returns:
        sqlalchemy.engine.Engine: An SQLAlchemy Engine object representing the database connection.

    Raises:
        Any exceptions raised by `load_env` or `sqlalchemy.create_engine` during the process.
    """
    db_host, db_port, db_name, db_user, db_password = load_env()
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    return engine

def load_data(csv_filename, engine, tablename, columns) -> None:
    """
    Load CSV data from CSV file into a tablename in SQL database.

    Raises:
        ValueError: If the 'car_data.csv' or 'consumer_data.csv' file is empty.
        ValueError: If 'car_data' or 'consumer_data' tables already exist in the database to prevent data duplication.
        FileNotFoundError: If there is an issue locating the CSV files.
        Exception: If an unexpected error occurs during the data loading process.
    """
    # Load CSV data
    df_data = pd.read_csv(csv_filename, names=columns, skiprows=1)
    if df_data.empty:
        raise ValueError(f"The '{csv_filename}' file is empty.")
    
    # Write data to SQL tables
    df_data.to_sql(tablename, engine, if_exists='append', index=False)

    print(f"Data '{csv_filename}' loaded succesfully into the base {tablename}")

def run_query(engine, sql_query:str) -> pd.DataFrame:
    """Executes a SQL query on a PostgreSQL database and returns the result as a Pandas DataFrame.

    Args:
        sql_query (str): The SQL query to be executed.

    Returns:
        pandas.DataFrame: A DataFrame containing the result of the SQL query.

    Raises:
        Any exceptions raised during the process of building the database engine or executing the query.
    """
    result_df = pd.read_sql_query(sql_query, engine)
    return result_df

def plot_QpYpET(engine) -> None:
    """Generates a bar plot showing the quantity sold per year and engine type.

    This function retrieves relevant data from a PostgreSQL database using the `run_query` function
    and then plots the results using seaborn and matplotlib.

    The SQL query selects data on the total sales volume, grouped by year and engine type.

    The resulting bar plot displays the quantity sold per year, with different colors representing
    the various engine types.

    Raises:
        Any exceptions raised during the process of querying the database or plotting the data.
    """

    # SQL query
    sql_query = """
    SELECT consumer_data."Year", car_data."Engine_Type", SUM(consumer_data."Sales_Volume") as total
    FROM car_data
    INNER JOIN consumer_data ON car_data."Make" = consumer_data."Make"
                            AND car_data."Model" = consumer_data."Model"
    GROUP BY consumer_data."Year", car_data."Engine_Type"
    ORDER BY consumer_data."Year";
    """

    df = run_query(engine, sql_query)

    plt.figure(figsize=(10, 10))
    sns.barplot(x='Year', y='total', hue='Engine_Type', data=df)
    plt.title('Quantity Sold per Year and Engine Type')
    plt.legend(title='Engine Type', title_fontsize='14', fontsize='12', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()