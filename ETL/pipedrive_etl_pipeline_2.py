import requests
import psycopg2

import datetime

from config import API_TOKEN, COMPANY_DOMAIN, DEALS_PER_PAGE, DBNAME, USER, PASSWORD, column_names


def extract_deals_from_pipedrive(limit=DEALS_PER_PAGE, start=0) -> list:
    """Function to retrieve deals data from Pipedrive API.

    Parameters
    ------------
        limit: int
            Number of deals displayed on a page.
        start: int
            The number from which the count of deals begins.
    Return
    ------------
        deals: list
            List of deals with all the additional info.
    """
    print(f"Getting Deals, limit: {limit}, start: {start}")

    url = f"https://{COMPANY_DOMAIN}.pipedrive.com/api/v1/deals?api_token={API_TOKEN}&start={start}&limit={limit}"

    response = requests.get(url)
    result = response.json()
    deals = []

    if 'data' in result and result['data']:
        deals.extend(result['data'])
    else:
        print(result)

    if 'more_items_in_collection' in result['additional_data']['pagination'] and result['additional_data']['pagination']['more_items_in_collection']:
        deals.extend(extract_deals_from_pipedrive(limit, result['additional_data']['pagination']['next_start']))

    return deals


def creator_user_data_extraction() -> list:
    """Function extracts creator_user data from deals and converts it to the list of tuples.
    It is necessary due to nesting in the source data.

    Return
    --------------
        Returns list of tuples. Tuples are the required format to pass to the query.
    """
    return [tuple(deal['creator_user_id'].values()) if deal['creator_user_id'] is not None else tuple(
        [None, None, None, None, None, None, None]) for deal in deals]


def user_data_extraction() -> list:
    """Function extracts user data from deals and converts it to the list of tuples.
    It is necessary due to nesting in the source data.

    Return
    --------------
        Returns list of tuples. Tuples are the required format to pass to the query
    """
    return [tuple(deal['user_id'].values()) if deal['user_id'] is not None else tuple(
        [None, None, None, None, None, None, None]) for deal in deals]


def person_data_extraction() -> list:
    """Function extracts person data from deals and converts it to the list of tuples.
    It is necessary due to nesting in the source data.

    Return
    --------------
        Returns list of tuples. Tuples are the required format to pass to the query
    """
    person_lst = []
    for deal in deals:
        person_id = deal.get('person_id')  # we get the key person_id

        if person_id is not None:
            if 'email' in person_id:
                email = person_id['email'][0].get('value')  # extract data from nested structure
                email_primary = person_id['email'][0].get('primary')

            if 'phone' in person_id:
                phone_label = person_id['phone'][0].get('label')  # extract data from nested structure
                phone_value = person_id['phone'][0].get('value')
                phone_primary = person_id['phone'][0].get('primary')

            active_flag = person_id['active_flag']  # these attributes are the same as there are no nested values in them
            name = person_id['name']
            owner_id = person_id['owner_id']
            value = person_id['value']
            person_lst.append(
                (active_flag, name, email, email_primary, phone_label, phone_value, phone_primary, owner_id, value))
        else:
            person_lst.append((None, None, None, None, None, None, None, None, None))
    return person_lst


def org_data_extraction() -> list:
    """Function extracts user data from deals and converts it to the list of tuples.
    It is necessary due to nesting in the source data.

    Return
    --------------
        Returns list of tuples. Tuples are the required format to pass to the query
    """
    return [tuple(deal['org_id'].values()) if deal['org_id'] is not None else tuple(
        [None, None, None, None, None, None, None, None]) for deal in deals]


def deals_data_transformation() -> list:
    """Function prepares deals data for further query.

    Return
    --------------
        deals_lst: list
            Returns list of deals.
    """
    deals_lst = []
    for deal in deals:
        deal['creator_user_id'] = dict(list(deal.values())[1])['id']
        deal['user_id'] = dict(list(deal.values())[2])['id']
        if deal['person_id'] is not None:
            deal['person_id'] = dict(list(deal.values())[3])['value']
        if deal['org_id'] is not None:
            deal['org_id'] = dict(list(deal.values())[4])['value']
        deals_lst.append(list(deal.values()))
    return deals_lst


def extend_deals(lst) -> list:
    """Function extends deals list with the other lists.

    Parameters
    ------------
        lst: list
            The list which will be used to extend the deals list.

    Return
    ------------
        deals: list
            The extended deals list.
    """
    for d, l in zip(deals, lst):
        d.extend(l)
    return deals


def load_data_to_postgres(t):
    """Function creates a connection to postgres DB, creates deals table in it and loads data into it.

    Parameters
    -------------
        t: list
            List of tuples which is passed to the query.
    """
    try:
        conn = psycopg2.connect(f"""dbname={DBNAME}
                                    user={USER} 
                                    password={PASSWORD}""")
        cur = conn.cursor()

        data_type_mapping = {
            int: 'INTEGER',
            float: 'NUMERIC',
            str: 'VARCHAR(500)',
            bool: 'BOOLEAN',
            datetime.datetime: 'TIMESTAMP',
            datetime.date: 'DATE',
            datetime.time: 'TIME',
            bytes: 'BYTEA',
        }

        column_types = []

        for value in t[0]:
            python_type = type(value)
            postgres_type = data_type_mapping.get(python_type, 'VARCHAR(500)')  # Default to VARCHAR(500) if type not found
            column_types.append(postgres_type)

        column_types[0] = "serial PRIMARY KEY"

        # Construct the CREATE TABLE statement with dynamic column definitions
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS test_schema.deals (
                                    {', '.join([f"{name} {data_type}" for name, data_type in zip(column_names, column_types)])}
                                );"""

        cur.execute(create_table_sql)

        cur.executemany("""INSERT INTO test_schema.deals
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (id) DO NOTHING;""", t)
        conn.commit()

        row_count = cur.rowcount
        print(row_count, "Records Updated")

    except (Exception, psycopg2.Error) as error:

        print("Error while updating PostgreSQL table", error)

    finally:
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":

    #Extract all the deals data from Pipedrive API
    deals = extract_deals_from_pipedrive()

    #Extract specific data from the deals
    creator_user_lst = creator_user_data_extraction()
    user_lst = user_data_extraction()
    org_lst = org_data_extraction()
    person_lst = person_data_extraction()

    # Transform deals data
    deals = deals_data_transformation()

    # Extend deals with the data from the lists created earlier
    deals = extend_deals(creator_user_lst)
    deals = extend_deals(user_lst)
    deals = extend_deals(org_lst)
    deals = extend_deals(person_lst)

    #Load deals data to postgres
    t = tuple(tuple(d) for d in deals)  # Tuple of tuples, the required format to pass to the query
    load_data_to_postgres(t)