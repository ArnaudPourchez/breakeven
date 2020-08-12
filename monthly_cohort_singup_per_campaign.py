# 2020 Arnaud Pourchez

import os
import json
import pandas as pd

from datetime import datetime
from typing import List, Optional
from campaign_list import campaign_list_func
from quick_date_range import date_range_report
from inhouse.connection_library.db_connections import db_connections


def report_monthly_cohort_signup(
                    date_range:List,
                    credential_path:str,
                    to_db:Optional[bool] = False
                                ) -> pd.DataFrame:
    """
    Function that gets and append monthly signup cohorts according to specific campaings_id
    from a specified date to the current date.
    It could either save the table in a database by replacing the table if it does not exist
    or save it as a CSV in a data folder.
    """

    # Getting the credentials in a json file (could be also in the environment variables)
    with open(credential_path) as f:
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

    # Connection to the db that we could create/update the new table
    connection_report = db_connections().create_external_connection(
        host=production["host"],
        port=production["port"],
        dbname="db4",
        user=production["user"],
        password=production["password"]
    )
   
    # Setting up internal parameters
    today = datetime.today().strftime('%Y-%m-%d')
    table = pd.DataFrame()
    columns = [
            "CC_revenue", 
            "Email_revenue", 
            "all_revenue"
            ]
    # Getting the list of campaigns and preparing the iterators
    campaigns = campaign_list_func(date_range[0], cred_path=credential_path)
    campaign_list = list(campaigns["campaign_id_corrected"])
    campaign_name = list(campaigns["name"])
    for camp, name in zip(campaign_list, campaign_name):
        print(f"Looking at {name} , campaign_id {camp} now")
        for date in date_range:
            query1 = f"""
            SELECT
                dt.campaign_id,
                date_format(transDate, '%%Y-%%m') AS month,
                SUM(CASE WHEN user_type = "CC" THEN netAmt END) AS CC_revenue,
                SUM(CASE WHEN user_type = "Email" THEN netAmt END) AS Email_revenue,
                SUM(netAmt) AS all_revenue,
                co.cost AS cost
            FROM
                db1.dailytransactions AS dt
                INNER JOIN db2.products AS p ON dt.product_id = p.id AND p.condition2 = 2
                INNER JOIN
                (
                    SELECT DISTINCT
                        foreignUserId,
                        CASE WHEN d.transactionType_id = 4 THEN "CC"
                            WHEN d.transactionType_id = 3 THEN "Email"
                            END AS user_type
                    FROM
                        db1.dailytransactions AS d
                        INNER JOIN db2.products AS p ON d.product_id = p.id AND p.condition2 = 2
                    WHERE
                        transDate BETWEEN "{date}" AND LAST_DAY("{date}")
                        AND condition1 IN (3,4)
                        AND foreignUserId <> ''  
                        AND foreignUserId IS NOT NULL
                        AND d.campaign_id = {camp}
                ) AS ms ON ms.foreignUserId = dt.foreignUserId
                LEFT JOIN
                    (
                    SELECT
                        date_format(costDate,'%%Y-%%m') AS cohort,
                        SUM(cost) AS cost
                    FROM
                        db1.summary_cost AS sc 
                    WHERE
                        costDate  BETWEEN "{date}" AND LAST_DAY("{date}")
                        AND campaign_id = {camp}
                    GROUP BY
                        date_format(costDate,'%%Y-%%m')
                    ) AS co ON co.cohort = date_format(dt.transDate, '%%Y-%%m')    
            WHERE
                transDate BETWEEN "{date}" AND CURRENT_DATE
                AND dt.foreignUserID <> ''  
                AND dt.foreignUserID IS NOT NULL
                AND dt.campaign_id = {camp}
            GROUP BY	
                date_format(transDate, '%%Y-%%m'),
                dt.campaign_id;
         """    

            sub_table = db_connections().query_to_db(
                                    connection=connection, 
                                    query=query1)
            if sub_table.empty:
                pass
            else:
                intermediate = sub_table
                intermediate["cohort"] = date
                intermediate["campaignName"] = name
                intermediate_sub_cumsum = intermediate.cumsum()
                for col in columns:
                    intermediate[f"{col}_cumsum"] = intermediate_sub_cumsum[col]
                table = pd.concat([table, intermediate], ignore_index=True, axis=0, sort=False)

    if to_db:
        table.to_sql("signup_cohort_by_campaign_v0", con=connection_report, index=False, if_exists="replace")
    else:
        if not os.path.exists("data/"):
            os.mkdir("data/")
        table.to_csv(f"data/paid_acquisition_spending_monthly_cohort_{today}.csv", index=False)
    
    return print("Done")



if __name__ == "__main__":

    cred = "../../sql_server_credentials.json"

    date_range = date_range_report(year=2017, month_nbr=1)

    report_monthly_cohort_signup(
            date_range=date_range,
            credential_path=cred)