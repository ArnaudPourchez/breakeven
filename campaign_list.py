# 2020 Arnaud Pourchez

import json
import pandas as pd

from inhouse.connection_library.db_connections import db_connections

def campaign_list_func(first_date:str, cred_path:str) -> pd.DataFrame:
    """
    Small function that gets the required campaigns names and id on interest
    according the different criteria that could be changed in the sql query itself.
    As of now, this function only required to the first date of the data range and
    the path for the credentials.
    """
    # Getting the credentials in a json file (could be also in the environment variables)
    with open(cred_path) as f:
        creds = json.load(f) 
    production = creds["Prod"]

    # Connection to the db that we will need
    connection = db_connections().create_external_connection(
        host=production["host"],
        port=production["port"],
        dbname="db3",
        user=production["user"],
        password=production["password"]
    )
    # Defining the query
    campaign_query = f"""
            SELECT
                c.name,
                dt.campaign_id
            FROM
                db3.campaigns AS c
            INNER JOIN 
            (
                SELECT DISTINCT
                    d.campaign_id
                FROM
                    db1.dailytransactions AS d
                    INNER JOIN db3.products AS p ON d.product_id = p.id AND p.condition2 = 2
                    INNER JOIN db1.summary_cost AS sc
                        ON sc.costDate = d.transDate
                        AND sc.campaign_id = d.campaign_id
                WHERE
                    transDate BETWEEN "{first_date}" AND CURRENT_DATE
                    AND d.condition1 IN (3, 4)
                    AND foreignUserID <> ''  
                    AND foreignUserID IS NOT NULL
                    ) AS dt ON dt.campaign_id = c.id
            GROUP BY
                dt.campaign_id;
        """
    campaigns = db_connections().query_to_db(
                                    connection=connection, 
                                    query=campaign_query)
    # Cleaning up a bit the data                                
    campaigns["campaign_id"] = campaigns["campaign_id"].astype(str)              
    campaigns = campaigns.iloc[1:]
    campaigns["campaign_id_corrected"] = list([camp.replace(".0", "") for camp in campaigns["campaign_id"]])
    
    return campaigns