"""
requirement:
Getting webhook log for two events (COMPLETE_REGISTRATION and CLICK_AD)
6 logs per day between 17 and 23 for the two above events respectively
Presto SQL does not support looping or variables, so we use Python!
"""

import prestodb
import pandas as pd
import os.path
from dotenv import load_dotenv

load_dotenv()
username = os.environ.get('USERNAME', 'your.name@branch.io')
password = os.environ.get('PASSWORD', 'your_branch_password')

conn = prestodb.dbapi.connect(
    host='presto-internal.prod.branch.io',
    port=443,
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication(
        username, password),
)
cur = conn.cursor()

days = range(17,24)
names = ['COMPLETE_REGISTRATION','CLICK_AD']

print(F"Retrieving reports...")
all = pd.DataFrame()
for day in days:
    for name in names:
        query = F"""
        select d, from_unixtime(timestamp/1000) as time_utc, name, app_name,
        webhook_partner_key, webhook_request_url, 
        webhook_response_success, webhook_response_code, webhook_response_body 
        from hive.warehouse.webhook2 
        where y = 2022 and m = 11 and d = {day}
        and app_id = 620368457443070072
        and name = '{name}'
        and webhook_partner_key = 'a_applovin'
        limit 6
        """
        cur.execute(query)
        rows = cur.fetchall()
        df = pd.DataFrame.from_records(
            rows, columns=[i[0] for i in cur.description])
        all = pd.concat([all, df])
        print(F"Retrieved report for {name} on {day}")
all.to_csv(F"webhooks_applovin_17_23.csv", index=False)
print(all.head())