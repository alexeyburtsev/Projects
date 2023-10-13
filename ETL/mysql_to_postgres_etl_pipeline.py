import pandas as pd
import mysql.connector
import psycopg2
import psycopg2.extras as extras

import logging

import warnings

warnings.simplefilter("ignore", UserWarning)

logging.basicConfig(level=logging.INFO, filename="mysql_to_postgres_etl.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")


def get_conn_mysql():
    """The function establishes connection to MySQL database (source database).

    Return
    --------
        cur:
            Cursor, allows Python code to execute PostgreSQL command in a database session.
        conn:
            Connection, handles the connection to a PostgreSQL database instance.
    """

    try:
        conn = mysql.connector.connect(host="localhost",
                                       port=3306,
                                       user="root",
                                       password="12345",
                                       db="classicmodels")

        cur = conn.cursor()
        logging.info("Connected to the MySQL database successfully.")

    except (Exception,  mysql.connector.Error) as error:
        logging.error(f"Error while connecting to MySQL database: {error}", exc_info=True)

    return cur, conn


def get_conn_postgresql():
    """The function establishes connection to PostgreSQL database (target database).

    Return
    --------
        cur:
            Cursor, allows Python code to execute PostgreSQL command in a database session.
        conn:
            Connection, handles the connection to a PostgreSQL database instance.
    """


    try:
        conn = psycopg2.connect(host="localhost",
                                database="postgres",
                                user="postgres",
                                password="12345")
        cur = conn.cursor()
        logging.info("Connected to the PostreSQL database successfully.")

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error while connecting to PostgreSQL database: {error}", exc_info=True)

    return cur, conn


def create_tables_in_postgres(cur2, conn2):
    """The function creates tables in the target database which will be populated later.

    Parameters
    ------------
        cur2:
            Cursor, allows Python code to execute PostgreSQL command in a database session.
        conn2:
            Connection, handles the connection to a PostgreSQL database instance.
    """

    commands = (
        """
        CREATE TABLE IF NOT EXISTS toporders(
            customername VARCHAR(255),
            number_of_orders INTEGER
        )
        """,
        """ CREATE TABLE IF NOT EXISTS product_demand(
                productName VARCHAR(255),
                quantity_ordered INTEGER
                )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_spending(
            customername VARCHAR(255),
            total_amount_spent float8
        )
        """,
        """
        TRUNCATE TABLE toporders, product_demand, customer_spending;
        """
    )
    # executing the queries against the target database
    try:
        for command in commands:
            cur2.execute(command)
        logging.info("--------- tables updated ----------")
        conn2.commit()

        # showing tables present in the target database
        cur2.execute("""SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public'""")

        for table in cur2.fetchall():
            logging.info(f"{table}")

    except (Exception, psycopg2.Error) as error:
            logging.error(f"Error while creating PostgreSQL tables: {error}", exc_info=True)


def extract_data_from_source_db(conn1):
    """The function extracts data from the target database and creates 3 dataframes from the
    results of SELECT statements.

    Parameters
    -----------
        conn1:
            Connection, handles the connection to a PostgreSQL database instance.

    Return
    --------
        df1, df2, df3:
            Dataframes created from the SELECT queries results.
    """

    # product demand-products with the highest purchases
    query1 = "SELECT productName , SUM(quantityOrdered) AS quantity_ordered\
           FROM  products, orderdetails\
           WHERE products.productCode = orderdetails.productCode\
           GROUP BY productName\
           ORDER BY quantity_ordered DESC\
           LIMIT 20;"

    # toporders - customers who have the most orders
    query2 = "SELECT contactFirstName, contactLastName , COUNT(*) AS number_of_orders\
           FROM  customers, orders\
           WHERE customers.customerNumber = orders.customerNumber\
           GROUP BY contactFirstName, contactLastName\
           ORDER BY number_of_orders DESC\
           LIMIT 20;"

    # customer spending - customers who have spent more
    query3 = "SELECT contactFirstName , contactLastName, SUM(quantityOrdered*priceEach) AS total_amount_spent\
           FROM  customers, orders, orderdetails\
           WHERE customers.customerNumber = orders.customerNumber AND orderdetails.orderNumber= orders.orderNumber\
           GROUP BY contactFirstName, contactLastName\
           ORDER BY total_amount_spent DESC\
           LIMIT 10;"

    # creating dataframes from the queries
    df1 = pd.read_sql(query1, con=conn1)
    df2 = pd.read_sql(query2, con=conn1)
    df3 = pd.read_sql(query3, con=conn1)

    logging.info(f"Dataframes are successfully created as result of SELECT queries from source DB.")

    return df1, df2, df3


def transform_data_in_dataframes(df1, df2, df3):
    """The function performs some transformations on the dataframes- joining columns
    for first name and last name

    Parameters
    -----------
        df1, df2, df3:
            Dataframes returned by extract_data_from_source_db function.

    Return
    --------
        df1, df2, df3:
            Transformed dataframes.
    """

    df2['customername'] = df2['contactFirstName'].str.cat(df2['contactLastName'], sep=" ")
    df2 = df2.drop(['contactFirstName', 'contactLastName'], axis=1)

    df3['customername'] = df3['contactFirstName'].str.cat(df3['contactLastName'], sep=" ")
    df3 = df3.drop(['contactFirstName', 'contactLastName'], axis=1)

    data_types = {'quantity_ordered': int}
    df1 = df1.astype(data_types)

    logging.info(f"Transformations over the dataframes are successfully performed.")

    return df1, df2, df3


def load_data_to_target_db(conn, cur, df, table):
    """The function loads data to the target database.

    Parameters
    -------------
        conn:
            Connection to the target DB.
        cur:
            Cursor which allows to perform queries in the target DB.
        df:
            Dataframe which will be converted to a table in a target DB.
        table:
            Table which will be created from the dataframe df.
    """

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))

    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    try:
        extras.execute_values(cur, query, tuples)
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error while creating PostgreSQL tables: {error}", exc_info=True)
        conn.rollback()
        return 1

    logging.info(f"-------data updated/inserted into {table}----")


def main():
    cur1, conn1 = get_conn_mysql()
    cur2, conn2 = get_conn_postgresql()
    create_tables_in_postgres(cur2, conn2)
    df1, df2, df3 = extract_data_from_source_db(conn1)
    df1, df2, df3 = transform_data_in_dataframes(df1, df2, df3)
    load_data_to_target_db(conn2, cur2, df1, 'product_demand')
    load_data_to_target_db(conn2, cur2, df2, 'toporders')
    load_data_to_target_db(conn2, cur2, df3, 'customer_spending')
    conn1.close()
    conn2.close()

if __name__ == "__main__":
    main()

