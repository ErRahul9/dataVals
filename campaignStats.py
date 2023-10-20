import argparse
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

from deviceTypeCampaignStats import getLineItemId


# --campaign_group_id 54772 --start_date "2023-10-12 00:00:00" --end_date "2023-10-13 00:00:00"
class campaignStats():
    load_dotenv()

    def __init__(self,start_date,end_date,campaign_id=None,campaign_group_id=None):
        self.core_host = os.environ["CORE_HOST"]
        self.core_user = os.environ["CORE_USER"]
        self.core_pwd = os.environ["CORE_PW"]
        self.core_port = os.environ["CORE_PORT"]
        self.core_db = os.environ["CORE_DATABASE"]
        self.prod_host = os.environ["PROD_HOST"]
        self.prod_user = os.environ["PROD_USER"]
        self.prod_pwd = os.environ["PROD_PW"]
        self.prod_port = os.environ["PROD_PORT"]
        self.prod_db = os.environ["PROD_DATABASE"]
        self.start_date = start_date.split(" ")[0].strip()
        self.end_date = end_date.split(" ")[0].strip()
        self.start_time = start_date.strip()
        self.end_time = end_date.strip()
        self.campaign_id = campaign_id
        self.campaign_group_id = campaign_group_id





    def returnCoreDbEngine(self):
        database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(self.core_user, self.core_pwd, self.core_host, self.core_port, self.core_db)
        engine = create_engine(database_url)
        return engine


    def returnProdDbEngine(self):
        database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(self.prod_user, self.prod_pwd, self.prod_host, self.prod_port, self.prod_db)
        engine = create_engine(database_url)
        return engine

    def getCampaigns(self):
        cmp= []
        if self.campaign_id is None:
            engine = self.returnProdDbEngine()
            with engine.connect() as connection:
                result = connection.execute(
                    text("select campaign_id from campaigns where campaign_group_id = {0}".format(self.campaign_group_id)))
                for row in result:
                    cmp.append(row[0])
            return cmp

    def getLineItemId(self):
        lineItemId = []
        engine = self.returnProdDbEngine()
        if self.campaign_id is None:
            with engine.connect() as connection:
                result = connection.execute(
                    text("select line_item_id,steelhouse_id from sync.beeswax_campaign_mapping"
                         " where steelhouse_id in "
                         "(select campaign_id from campaigns "
                         "where campaign_group_id = {0});".format(self.campaign_group_id)))
                # for row in result:
                    # lineItemId.append(row[0])
                lineItemId = {row[0]: row[1] for row in result}

        else:
            with engine.connect() as connection:
                result = connection.execute(
                    text("select line_item_id,steelhouse_id from sync.beeswax_campaign_mapping where steelhouse_id in "
                         "(select campaign_id from campaigns where campaign_id = {0});".format(self.campaign_id)))
                # for row in result:
                lineItemId = {row[0]: row[1] for row in result}
        return lineItemId



    def read_sql_file(self,filename):
        with open(filename, 'r') as file:
            return file.read()


    def format_sql_query(self,query, **params):
        return query.format(**params)


    def getAdvertiser(self):
        campaign_id = []
        if self.campaign_id is None:
            campaign_id = self.getCampaigns()
        else:
            campaign_id.append(self.campaign_id)
        engine = self.returnCoreDbEngine()
        with engine.connect() as connection:
            result = connection.execute(
                text("select advertiser_id from campaigns where campaign_id = {0}".format(campaign_id[0])))
            for row in result:
                adv = row[0]
        return adv


    def campStats(self,camp):
        engine_core = self.returnCoreDbEngine()
        engine_prod = self.returnProdDbEngine()
        advertiser_id = self.getAdvertiser()
        params = {"campaign_id": camp, "advertiser_id": advertiser_id, "start_date": self.start_date,
                  "end_date": self.end_date, "start_time":self.start_time,"end_time":self.end_time}
        print(params)
        getBudget = text(self.read_sql_file('sql_files/budget.sql'))
        getSpend = text(self.read_sql_file('sql_files/spend.sql'))
        getfreq = text(self.read_sql_file('sql_files/dco.sql'))
        getrecency = text(self.read_sql_file('sql_files/recency.sql'))
        getTargeting = text(self.read_sql_file('sql_files/conversitons.sql'))
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
        return df_final_with_tgt


    def cmpGetHourlyStats(self,line_item_id,campaign_id):
        engine_core = self.returnCoreDbEngine()
        engine_prod = self.returnProdDbEngine()
        advertiser_id = self.getAdvertiser()

        params = {"line_item_id": line_item_id, "campaign_id": campaign_id, "advertiser_id": advertiser_id,
                  "start_date": self.start_date,
                  "end_date": self.end_date, "start_time": self.start_time, "end_time": self.end_time}
        getWins = text(self.read_sql_file('sql_files/bidWinStats.sql'))
        getBudget = text(self.read_sql_file('sql_files/device_budget.sql'))
        df_budget = pd.read_sql(getBudget,engine_prod,params=params)
        df_budget["line_item_id"] = df_budget["line_item_id"].astype('int64')
        df_wins_spends = pd.read_sql(getWins, engine_core, params=params).sort_values(by='bid_hour')
        df_wins_spends["line_item_id"] = df_wins_spends["line_item_id"].astype('int64')
        df_completeStats = df_wins_spends.merge(df_budget,on=["line_item_id"],how="outer")
        print(df_completeStats)
        order = ["advertiser_id","campaign_id","line_item_id",
                 "hourly_budget","device_type_budget_percent","devicetype",
                 "bid_hour",
                 "win_counts",
                 "hourly_spend"]
        ordered = df_completeStats[order]
        ordered.to_csv(f'reports/output_{line_item_id}_{campaign_id}.csv', index=False)
        # if len(ordered) > 0:
        #     df = ordered.merge(device_type_df, left_on="platform_device_type", right_on="DeviceType", how="left")
        #
        #     # Convert 'bid_hour' to datetime
        #     df['bid_hour'] = pd.to_datetime(df['bid_hour'])
        #
        #     # Extract the hour
        #     df['hour'] = df['bid_hour'].dt.hour
        #
        #     # Group by hour and platform_device_type, and sum the hourly spend
        #     grouped = df.groupby(['hour', 'platform_device_type'])['hourly_spend'].sum().unstack().fillna(0)
        #
        #     # Create a clustered column chart
        #     fig, ax = plt.subplots(figsize=(12, 6))
        #     grouped.plot(kind='bar', ax=ax)
        #
        #     # Set y-axis gaps of 1000
        #     y_ticks = np.arange(0, grouped.values.max() + 2000, 2000)
        #     ax.set_yticks(y_ticks)
        #
        #     plt.xlabel('Hour of the Day')
        #     plt.ylabel('Hourly Spend')
        #     plt.title('Hourly Spend by Platform Device Type')
        #     plt.legend(title='Platform Device Type')
        #
        #     plt.show()

            # ordered['hour'] = ordered['bid_hour'].dt.hour
            # grouped = ordered.groupby(['hour', 'devicetype'])['hourly_spend'].sum().unstack().fillna(0)
            # fig, ax = plt.subplots(figsize=(12, 6))
            # grouped.plot(kind='bar', ax=ax)
            # plt.xlabel('Hour of the Day')
            # plt.ylabel('Hourly Spend')
            # plt.title(
            #     f'Hourly Spend by Platform Device Type {df_wins_spends["bid_hour"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")}')
            # plt.legend(title='Platform Device Type')
            # current_time = datetime.datetime.now()
            # timestamp = current_time.strftime("%Y%m%d%H%M%S")
            # filename = f"{campaign_id}_{timestamp}.png"
            # plt.savefig(filename)
            # plt.show()
        # else:
        #     print("skipping plot")
        return ordered





    def runMultiplDays(self):
        engine_core = self.returnCoreDbEngine()
        getspend = text(self.read_sql_file('sql_files/spendDataDays.sql'))
        df_spend = pd.read_sql(getspend, engine_core)
        df_spend["perChangeDay0"] = ((df_spend["spend_26"] - df_spend["spend_25"]) / df_spend["spend_25"]) * 100
        df_spend["perChangeDay1"] = ((df_spend["spend_27"] - df_spend["spend_26"]) / df_spend["spend_26"]) * 100
        df_spend["perChangeDay2"] = ((df_spend["spend_28"] - df_spend["spend_27"]) / df_spend["spend_27"]) * 100
        df_spend["perChangeDay3"] = ((df_spend["spend_29"] - df_spend["spend_28"]) / df_spend["spend_28"]) * 100
        df_sorted = df_spend.sort_values(by=['spend_27'], ascending=[False])
        filtered_df = df_sorted[(df_sorted['spend_25'] > .1) | (df_sorted['spend_26'] > .1)]
        print(tabulate(filtered_df, headers='keys', tablefmt='grid'))
        filtered_df.to_csv('output.csv', index=False)



# if __name__ == "__main__":
    # if args.campaign_id is not None:
    #     cmp = [args.campaign_id]
    # else:
    #     cmp = getCampaigns()
    # lines = getLineItemId()
    # for line,camps in lines.items():
    #     campaignStats(camps)
#         print(line,camps)
#         cmpGetHourlyStats(line,camps)