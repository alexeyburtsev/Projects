import requests
import pandas as pd
import logging
from io import StringIO

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from config import API_KEY, CONTACTS_PER_PAGE, MAX_RESULTS, USER, DBNAME, PORT, PASSWORD

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

logging.basicConfig(level=logging.INFO, filename="hubspot_etl.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")



buf = StringIO()

def extract_contacts_from_hubspot(limit=CONTACTS_PER_PAGE, after=0) -> list:
    """Function makes api requests to hubspot CRM and gets the contacts data with their properties.

    Parameters
    -----------
        limit: int
            Number of contacts displayed on a page (max = 100).
        after: int
            The paging cursor token of the last successfully read resource which is returned as the paging.next.after
            JSON property of a paged response containing more results.
    Return
    -----------
        all_contacts: list
            List of contacts with their properties.
    """

    base_url = "https://api.hubspot.com/crm/v3/objects/contacts"
    properties = "?properties=email,firstname,lastname,phone,month,lifecycle_stage_2_use,lifecyclestage,original_source,pipedrive_source,lead_rejection_reason,country,company"

    url = base_url + properties

    all_contacts = []

    while True:
        params = {
            "limit": limit,
            "after": after,
        }

        headers = {
            "Authorization": f"Bearer {API_KEY}",
        }

        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json()
            contacts_page = data.get("results", [])
            if not contacts_page:
                break

            all_contacts.extend(contacts_page)

            if 'paging' in data and 'next' in data['paging']:
                after = data['paging']['next']['after']

            if len(all_contacts) >= MAX_RESULTS:
                logging.info('Maximum number of results exceeded')
                break
        else:
            logging.info(f"Failed to fetch data from HubSpot. Status code: {response.status_code}")
            raise Exception(f"Failed to fetch data from HubSpot. Status code: {response.status_code}")

    return all_contacts


def transform_contacts_data(contact_list):
    """Function selects the required contact properties from the file and creates a pandas dataframe
    with all the properties included.

    Parameters
    ------------
        contact_list: list
            List of dictionaries (1 dictionary per contact).
    Return
    ------------
        contact_df: dataframe
            A pandas dataframe which will be later converted to a Postgres table.
    """
    contact_df = pd.DataFrame(columns=['id', 'createdate', 'firstname',
                                       'lastname', 'email', 'country',
                                       'company', 'lastmodifieddate', 'hs_object_id',
                                       'lead_rejection_reason', 'lifecyclestage',
                                       'lifecycle_stage_2_use', 'phone', 'pipedrive_source'])
    for cnt in contact_list:
        temp = {}

        if 'id' in cnt.keys():
            id = cnt['id']

        prop = cnt["properties"]

        if 'createdate' in prop.keys():
            createdate = prop['createdate']
        if 'firstname' in prop.keys():
            firstname = prop['firstname']
        if 'lastname' in prop.keys():
            lastname = prop['lastname']
        if 'email' in prop.keys():
            email = prop['email']
        if 'country' in prop.keys():
            country = prop['country']
        if 'company' in prop.keys():
            company = prop['company']
        if 'lastmodifieddate' in prop.keys():
            lastmodifieddate = prop['lastmodifieddate']
        if 'hs_object_id' in prop.keys():
            hs_object_id = prop['hs_object_id']
        if 'lead_rejection_reason' in prop.keys():
            lead_rejection_reason = prop['lead_rejection_reason']
        if 'lifecyclestage' in prop.keys():
            lifecyclestage = prop['lifecyclestage']
        if 'lifecycle_stage_2_use' in prop.keys():
            lifecycle_stage_2_use = prop['lifecycle_stage_2_use']
        if 'phone' in prop.keys():
            phone = prop['phone']
        if 'pipedrive_source' in prop.keys():
            pipedrive_source = prop['pipedrive_source']

        temp.update({"id": int(id)})
        temp.update({"createdate": createdate})
        temp.update({"firstname": firstname})
        temp.update({"lastname": lastname})
        temp.update({"email": email})
        temp.update({"country": country})
        temp.update({"company": company})
        temp.update({"lastmodifieddate": lastmodifieddate})
        temp.update({"hs_object_id": hs_object_id})
        temp.update({"lead_rejection_reason": lead_rejection_reason})
        temp.update({"lifecyclestage": lifecyclestage})
        temp.update({"lifecycle_stage_2_use": lifecycle_stage_2_use})
        temp.update({"phone": phone})
        temp.update({"pipedrive_source": pipedrive_source})
        contact_df = contact_df.append(temp, ignore_index=True)

    contact_df.info(buf=buf)
    logging.info(f"""Contacts dataframe with shape {contact_df.shape} is successfully created. 
                     More information: {buf.getvalue()}""")

    return contact_df


def load_contacts_data(contact_df):
    """Function creates a connection to postgres DB, transforms previously created dataframe
    and creates a table in a DB from the dataframe.

    Parameters
    -------------
        contact_df: dataframe
            A dataframe which includes contacts and their properties.
    """
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            port=PORT,
            database=DBNAME
        )
        cursor = connection.cursor()
        logging.info("Connected to the database successfully.")

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error while connecting to PostgreSQL: {error}", exc_info=True)

    engine = create_engine("postgresql+psycopg2://" + USER + ":" + str(PASSWORD) + "@localhost:" + PORT + "/" + DBNAME)

    contact_df.to_sql('contacts', engine, if_exists='append', index=False)

    logging.info("Contacts dataframe is successfully converted to PostreSQL table.")

    if connection:
        cursor.close()
        connection.close()
        logging.info("PostgreSQL connection is closed.")


if __name__ == "__main__":
    contact_list = extract_contacts_from_hubspot()
    contact_df = transform_contacts_data(contact_list)
    load_contacts_data(contact_df)