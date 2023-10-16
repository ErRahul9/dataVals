# import os
#
# from flask import Flask, render_template_string
# import pandas as pd
# import sqlite3
# from dotenv import load_dotenv
# from sqlalchemy import create_engine, text
#
# load_dotenv()
# core_host = os.environ["CORE_HOST"]
# core_user = os.environ["CORE_USER"]
# core_pwd = os.environ["CORE_PW"]
# core_port = os.environ["CORE_PORT"]
# core_db = os.environ["CORE_DATABASE"]
#
# prod_host = os.environ["PROD_HOST"]
# prod_user = os.environ["PROD_USER"]
# prod_pwd = os.environ["PROD_PW"]
# prod_port = os.environ["PROD_PORT"]
# prod_db = os.environ["PROD_DATABASE"]
#
#
# def returnCoreDbEngine():
#     database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(core_user, core_pwd, core_host, core_port, core_db)
#     engine = create_engine(database_url)
#     return engine
#
#
# def returnProdDbEngine():
#     database_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(prod_user, prod_pwd, prod_host, prod_port, prod_db)
#     engine = create_engine(database_url)
#     return engine
# def read_sql_file(filename):
#     with open(filename, 'r') as file:
#         return file.read()
#
# def format_sql_query(query, **params):
#     return query.format(**params)
#
#
# def getAdvertiser():
#     adv = 0
#     engine = returnCoreDbEngine()
#     with engine.connect() as connection:
#         result = connection.execute(text("select advertiser_id from campaigns where campaign_id = {0}".format(campaign_id)))
#         for row in result:
#             adv = row[0]
#     return adv
#
#
#
# app = Flask(__name__)
#
# @app.route('/get_data')
# def get_data():
#     # Connect to your DB and run your query
#     # conn = sqlite3.connect('your_database.db')
#     # df = pd.read_sql('YOUR_QUERY_HERE', conn)
#     # conn.close()
#     engine_core = returnCoreDbEngine()
#     getspend = text(read_sql_file('sql_files/spendDataDays.sql'))
#     df_spend = pd.read_sql(getspend, engine_core)
#     df_spend["perChangeDay0"] = ((df_spend["spend_26"] - df_spend["spend_25"]) / df_spend["spend_25"]) * 100
#     # Convert DataFrame to HTML
#     table_html = df_spend.to_html(classes='data', header="true")
#
#     # Render and return
#     template = '''
#     <html>
#       <head>
#         <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
#       </head>
#       <body>
#         <div class="container mt-5">
#           {{ table_html | safe }}
#         </div>
#       </body>
#     </html>
#     '''
#     return render_template_string(template, table_html=table_html)
#
# if __name__ == "__main__":
#     app.run(host='localhost', port=5001, debug=True)
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/table')
def display_table():
    # You can pass dynamic data to this template too
    return render_template("prodIncidents/table_template.html")

if __name__ == "__main__":
    app.run(debug=True)