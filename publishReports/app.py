import os


from flask import Flask, render_template, request, send_file
from campaignStats import campaignStats

app = Flask(__name__)

template_folder = os.path.abspath('/Users/rahulparashar/PycharmProjects/prodIncidents/publishReports')
app = Flask(__name__, template_folder=template_folder)


# @app.route('/df_hourly_stats', methods=['GET','POST'])
@app.route('/df_hourly_stats')
def df_hourly_stats():
    if request.method == 'POST':
        startTime = request.form['starttime']
        endTime = request.form['endtime']
        campaign_id = request.form['campaign_id']
        data_processor = campaignStats(startTime, endTime, campaign_id)
        lines = data_processor.getLineItemId()
        for line, camps in lines.items():
            print(line,camps)
            df_hourly_stats =  data_processor.cmpGetHourlyStats(line,camps)
        return render_template('indexCss.html',df=df_hourly_stats)
    return render_template('indexCss.html')
#
# @app.route('/CampDeviceBudget', methods=['GET','POST'])
# def CmpBudgetStats():
#     if request.method == 'POST':
#         startTime = request.form['starttime']
#         endTime = request.form['endtime']
#         campaign_id = request.form['campaign_id']
#         data_processor = campaignStats(startTime, endTime, campaign_id)
#         lines = data_processor.getLineItemId()
#         for line, camps in lines.items():
#             print(line,camps)
#             df_stats =  data_processor.campStats(camps)
#         print(df_stats)
#         return render_template('index.html',df=df_stats)
#     return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
