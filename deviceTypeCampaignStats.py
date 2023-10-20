import argparse
import os
import datetime
import os
import sys

import numpy as np
import redis
from flask import render_template, Flask
from matplotlib import pyplot as plt
from tabulate import tabulate

from sqlalchemy import create_engine, text
import pandas as pd

from dotenv import load_dotenv
# from campaignStats import returnCoreDbEngine



load_dotenv()
core_host = os.environ["CORE_HOST"]
core_user = os.environ["CORE_USER"]
core_pwd = os.environ["CORE_PW"]
core_port = os.environ["CORE_PORT"]
core_db = os.environ["CORE_DATABASE"]
prod_host = os.environ["PROD_HOST"]
prod_user = os.environ["PROD_USER"]
prod_pwd = os.environ["PROD_PW"]
prod_port = os.environ["PROD_PORT"]
prod_db = os.environ["PROD_DATABASE"]


parser = argparse.ArgumentParser(description='pass following params to the script .')

# Add named arguments
parser.add_argument('--campaign_id', type=int, required=False, help='campaign_id to test')
parser.add_argument('--campaign_group_id', type=int, required=False, help='campaign_group_id to test')
parser.add_argument('--start_date', type=str,required=True, help='from_date')
parser.add_argument('--end_date', type=str,required=True, help='to_date')
args = parser.parse_args()
if args.campaign_id is None and args.campaign_group_id is None:
    parser.error("either campaign_id or campaign_group_id must be provided")
start_date = args.start_date.split(" ")[0].strip()
end_date = args.end_date.split(" ")[0].strip()
start_time = args.start_date.strip()
end_time = args.end_date.strip()

def returnCoreDbEngine():
    database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(core_user, core_pwd, core_host, core_port, core_db)
    engine = create_engine(database_url)
    return engine


def returnProdDbEngine():
    database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(prod_user, prod_pwd, prod_host, prod_port, prod_db)
    engine = create_engine(database_url)
    return engine

def getCampaigns():
    cmp= []
    if args.campaign_id is None:
        engine = returnProdDbEngine()
        with engine.connect() as connection:
            result = connection.execute(
                text("select campaign_id from campaigns where campaign_group_id = {0}".format(args.campaign_group_id)))
            for row in result:
                cmp.append(row[0])
        return cmp

def getLineItemId():
    lineItemId = []
    engine = returnProdDbEngine()
    if args.campaign_id is None:
        with engine.connect() as connection:
            result = connection.execute(
                text("select line_item_id,steelhouse_id from sync.beeswax_campaign_mapping"
                     " where steelhouse_id in "
                     "(select campaign_id from campaigns "
                     "where campaign_group_id = {0});".format(args.campaign_group_id)))
            # for row in result:
                # lineItemId.append(row[0])
            lineItemId = {row[0]: row[1] for row in result}

    else:
        with engine.connect() as connection:
            result = connection.execute(
                text("select line_item_id,steelhouse_id from sync.beeswax_campaign_mapping where steelhouse_id in "
                     "(select campaign_id from campaigns where campaign_id = {0});".format(args.campaign_id)))
            # for row in result:
            lineItemId = {row[0]: row[1] for row in result}
    return lineItemId



def read_sql_file(filename):
    with open(filename, 'r') as file:
        return file.read()


def format_sql_query(query, **params):
    return query.format(**params)


def getAdvertiser():
    campaign_id = []
    if args.campaign_id is None:
        campaign_id = getCampaigns()
    else:
        campaign_id.append(args.campaign_id)
    engine = returnCoreDbEngine()
    with engine.connect() as connection:
        result = connection.execute(
            text("select advertiser_id from campaigns where campaign_id = {0}".format(campaign_id[0])))
        for row in result:
            adv = row[0]
    return adv


def campStats(camp):
    engine_core = returnCoreDbEngine()
    engine_prod = returnProdDbEngine()
    advertiser_id = getAdvertiser()
    params = {"campaign_id": camp, "advertiser_id": advertiser_id, "start_date": start_date,
              "end_date": end_date, "start_time":start_time,"end_time":end_time}
    print(params)
    getBudget = text(read_sql_file('sql_files/budget.sql'))
    getSpend = text(read_sql_file('sql_files/spend.sql'))
    getfreq = text(read_sql_file('sql_files/dco.sql'))
    getrecency = text(read_sql_file('sql_files/recency.sql'))
    getTargeting = text(read_sql_file('sql_files/conversitons.sql'))
    df_budget = pd.read_sql(getBudget, engine_core, params=params)
    df_spend = pd.read_sql(getSpend, engine_core, params=params)
    df_freq = pd.read_sql(getfreq, engine_core, params=params)
    df_recency = pd.read_sql(getrecency, engine_prod, params=params)
    df_tgt = pd.read_sql(getTargeting, engine_core, params=params)
    df_S_B = pd.merge(df_budget,df_spend,on=['campaign_id','date'],how='outer')
    df_Spend_budget_freq = pd.merge(df_S_B,df_freq,on=['campaign_id','date'],how='outer')
    df_final = pd.merge(df_Spend_budget_freq,df_recency,on=['campaign_id','date'],how='outer')
    df_final_with_tgt = pd.merge(df_final,df_tgt,on=['campaign_id','date'],how='outer')
    df_final_with_tgt["over/Under"] = df_final_with_tgt["median_daily_budget"] - df_final_with_tgt["media_cost"]
    print(tabulate(df_final_with_tgt, headers='keys', tablefmt='grid'))
    df_final_with_tgt.to_csv(f'reports/output_{camp}_DCO_Spend.csv', index=False)
    return df_final_with_tgt


def cmpGetHourlyStats(line_item_id,campaign_id):
    engine_core = returnCoreDbEngine()
    engine_prod = returnProdDbEngine()
    advertiser_id = getAdvertiser()
    category_map = {"TV": ["CONNECTED_TV", "SET_TOP_BOX", "GAMES_CONSOLE"]}
    params = {"line_item_id": line_item_id, "campaign_id": campaign_id, "advertiser_id": advertiser_id,
              "start_date": start_date,
              "end_date": end_date, "start_time": start_time, "end_time": end_time}
    getWins = text(read_sql_file('sql_files/bidWinStats.sql'))
    getBudget = text(read_sql_file('sql_files/device_budget.sql'))
    df_budget = pd.read_sql(getBudget,engine_prod,params=params)
    df_budget["line_item_id"] = df_budget["line_item_id"].astype('int64')
    df_wins_spends = pd.read_sql(getWins, engine_core, params=params).sort_values(by='bid_hour')
    df_wins_spends["line_item_id"] = df_wins_spends["line_item_id"].astype('int64')
    df_wins_spends.rename(columns={'devicetype': 'deviceType'}, inplace=True)
    df_completeStats = df_wins_spends.merge(df_budget,on=["line_item_id"],how="outer")
    device_type_df = pd.DataFrame([
         {"deviceType": "CONNECTED_TV", "DeviceTypeGroup": "CONNECTED_TV"},
         {"deviceType": "CONNECTED_DEVICE", "DeviceTypeGroup": "CONNECTED_TV"},
         {"deviceType": "SET_TOP_BOX", "DeviceTypeGroup": "CONNECTED_TV"},
         {"deviceType": "GAMES_CONSOLE", "DeviceTypeGroup": "CONNECTED_TV"},
         {"deviceType": "MOBILE", "DeviceTypeGroup": "MOBILE"},
         {"deviceType": "PHONE", "DeviceTypeGroup": "MOBILE"},
         {"deviceType": "TABLET", "DeviceTypeGroup": "TABLET"},
         {"deviceType": "PC", "DeviceTypeGroup": "COMPUTER"},
         {"deviceType": "UNKNOWN", "DeviceTypeGroup": "UNKNOWN"},
    ])
    df = df_completeStats.merge(device_type_df, left_on="deviceType", right_on ="deviceType", how ="left")
    order = ["advertiser_id","campaign_id","line_item_id",
             "hourly_budget","device_type_budget_percent","deviceType","DeviceTypeGroup",
             "bid_hour",
             "win_counts",
             "hourly_spend"]
    ordered = df[order]
    ordered.to_csv(f'reports/output_{line_item_id}_{campaign_id}.csv', index=False)
    if len(ordered) > 0:
        ordered['hour'] = ordered['bid_hour'].dt.hour
        grouped = ordered.groupby(['hour', 'DeviceTypeGroup'])['hourly_spend'].sum().unstack().fillna(0)
        fig, ax = plt.subplots(figsize=(12, 6))
        grouped.plot(kind='bar', ax=ax)
        plt.xlabel('Hour of the Day')
        plt.ylabel('Hourly Spend')
        plt.title(
            f'Hourly Spend by Platform Device Type {df_wins_spends["bid_hour"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")}')
        plt.legend(title='Platform Device Type')
        current_time = datetime.datetime.now()
        timestamp = current_time.strftime("%Y%m%d%H%M%S")
        filename = f"reports/{campaign_id}_{timestamp}.png"
        plt.savefig(filename)
        plt.show()
    else:
        print("skipping plot")





def runMultiplDays():
    engine_core = returnCoreDbEngine()
    getspend = text(read_sql_file('sql_files/spendDataDays.sql'))
    df_spend = pd.read_sql(getspend, engine_core)
    df_spend["perChangeDay0"] = ((df_spend["spend_26"] - df_spend["spend_25"]) / df_spend["spend_25"]) * 100
    df_spend["perChangeDay1"] = ((df_spend["spend_27"] - df_spend["spend_26"]) / df_spend["spend_26"]) * 100
    df_spend["perChangeDay2"] = ((df_spend["spend_28"] - df_spend["spend_27"]) / df_spend["spend_27"]) * 100
    df_spend["perChangeDay3"] = ((df_spend["spend_29"] - df_spend["spend_28"]) / df_spend["spend_28"]) * 100
    df_sorted = df_spend.sort_values(by=['spend_27'], ascending=[False])
    filtered_df = df_sorted[(df_sorted['spend_25'] > .1) | (df_sorted['spend_26'] > .1)]
    print(tabulate(filtered_df, headers='keys', tablefmt='grid'))
    filtered_df.to_csv('output.csv', index=False)



if __name__ == "__main__":
    # if args.campaign_id is not None:
    #     cmp = [args.campaign_id]
    # else:
    #     cmp = getCampaigns()
    lines = getLineItemId()
    for line,camps in lines.items():
        print(line,camps)
        # campStats(camps)
        cmpGetHourlyStats(line,camps)