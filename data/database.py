"""
Insert module description
"""

import pickle
import psycopg2
import psycopg2.extras
from config.config import config_for_data_paths, config_for_database_connection


def load_parquet_file(path):
    # # Load pickled dataframes
    # order_book_df_path = "data/collective_train_order_book_df.pkl"
    # trade_book_df_path = "data/collective_train_trade_book_df.pkl"
    parquet_file = open(path, 'rb')
    df = pickle.load(parquet_file)
    df.drop(['index'], axis=1, inplace=True)
    return df


def get_table_names():
    connection_params = config_for_database_connection()
    conn = None
    try:
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                query_script = '''
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE';
                '''
                cursor.execute(query_script)
                return [table_name[0] for table_name in cursor.fetchall()]
    except Exception as e:
        print("Error encountered when retrieving table names")
        print(e)
    finally:
        if conn is not None:
            conn.close()


def drop_table(table_name=None, drop_all=False):
    # TODO: Include check if any tables present
    # Check if any tables are present first, return warning?
    # Est. Completion: -
    # Author - Xerxes
    # Date - 29/12/2022
    if not drop_all:
        if table_name in get_table_names():
            connection_params = config_for_database_connection()
            conn = None
            try:
                with psycopg2.connect(**connection_params) as conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                        drop_script = f'''
                            DROP TABLE {table_name}
                        '''
                        cursor.execute(drop_script)
                        print(f"Table {table_name} dropped")
            except Exception as e:
                print(f"Error encountered when dropping {table_name} table")
                print(e)
            finally:
                if conn is not None:
                    conn.close()
        else:
            print(f"Invalid table name: {table_name}")
    else:
        table_name = None
        table_names = get_table_names()
        connection_params = config_for_database_connection()
        conn = None
        try:
            with psycopg2.connect(**connection_params) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    for table_name in table_names:
                        drop_script = f'''
                            DROP TABLE {table_name}
                        '''
                        cursor.execute(drop_script)
                        print(f"Table {table_name} dropped")
                    print("All tables dropped")
        except Exception as e:
            print(f"Error encountered when dropping {table_name} table")
            print(e)
        finally:
            if conn is not None:
                conn.close()


def push_data_to_postgresql():
    connection_params = config_for_database_connection()
    path_params = config_for_data_paths()
    conn = None
    try:
        # Open connection to server
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                create_tables_script = '''
                    CREATE TABLE IF NOT EXISTS train_order_book(
                        id BIGSERIAL NOT NULL PRIMARY KEY,
                        time_id INT NOT NULL,
                        seconds_in_bucket INT NOT NULL,
                        bid_price1 FLOAT NOT NULL,
                        ask_price1 FLOAT NOT NULL,
                        bid_price2 FLOAT NOT NULL,
                        ask_price2 FLOAT NOT NULL,
                        bid_size1 INT NOT NULL,
                        ask_size1 INT NOT NULL,
                        bid_size2 INT NOT NULL,
                        ask_size2 INT NOT NULL
                        );
                    CREATE TABLE IF NOT EXISTS train_trade_book(
                        id BIGSERIAL NOT NULL PRIMARY KEY,
                        time_id INT NOT NULL,
                        seconds_in_bucket INT NOT NULL,
                        price FLOAT NOT NULL,
                        size INT NOT NULL,
                        order_count INT NOT NULL
                        );
                '''
                cursor.execute(create_tables_script)

                # Method 1: Insert via CSV import
                insert_order_book_script = '''
                    COPY train_order_book(
                        time_id,
                        seconds_in_bucket,
                        bid_price1,
                        ask_price1,
                        bid_price2,
                        ask_price2,
                        bid_size1,
                        ask_size1,
                        bid_size2,
                        ask_size2
                    )
                    FROM %s
                    DELIMITER ','
                    CSV HEADER;
                '''
                insert_trade_book_script = '''
                    COPY train_trade_book(
                        time_id,
                        seconds_in_bucket,
                        price,
                        size,
                        order_count
                    )
                    FROM %s
                    DELIMITER ','
                    CSV HEADER;
                '''
                cursor.execute(insert_order_book_script, (path_params['train_order_book_csv_path'],))
                cursor.execute(insert_trade_book_script, (path_params['train_trade_book_csv_path'],))
                print("Push to database successful")
    except Exception as e:
        print("Error encountered when pushing to database")
        print(e)
    finally:
        if conn is not None:
            conn.close()


def check_if_data_loaded():
    # TODO: Create check if csv data is already loaded into database
    # Could check if number of rows of csv files matches table rows
    # Explore ways to check rows without loading files into memory
    # Est. Completion: -
    # Author - Xerxes
    # Date - 29/12/2022
    pass
