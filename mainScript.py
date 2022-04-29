import os
import imp
import psycopg2
import pandas as pd
from multiprocessing import Process
from analytics.mayhem.adobe import analytics_client
from datetime import datetime
from datetime import date
from datetime import timedelta
from selenium import webdriver

# Adobe Analytics Oauth Client
ac = analytics_client(
    authClientId = os.getenv('CLIENT_ID'),
    clientSecret = os.getenv('CLIENT_SECRET'),
    accountId = os.getenv('GLOBAL_COMPANY_ID')
)
# END: Adobe Analytics Oauth Client

# Pull Data From Adobe
def pullDataFromAdobe(daysbehind):
    ad._authenticate()
    ad.set_report_suite(report_suite_id = os.getenv('VIRTUAL_REPORT_SUITE_ID'))
    aa.add_global_segment(segment_id = "s1689_5ea0ca222b1c1747636dc970")
    aa.add_metric(metric_name = 'metrics/visits')
    aa.add_metric(metric_name = 'metrics/orders')
    aa.add_metric(metric_name = 'metrics/event1')
    aa.add_dimension(dimension_name = 'variables/mobiledevicetype')

    day = datetime.strftime(date.today() - timedelta(days = daysbehind), '%Y-%m-%d')
    ad.set_date_range(date_start = day, date_end = day)

    # Store data into a dataframe
    data = ad.get_report_multiple_breakdowns()

    dayx = date.today()- timedelta(days = daysbehind)
    data_file = data.assign(month = dayx.month)
    data_file = data_file.assign(day = dayx.day)
    titles = list(data_file.columns)
    data_file = data_file[titles]
    data_file[["metrics/visits"]] = data_file[["metrics/visits"]].apply(pd.to_numeric, errors = 'coerce', downcast = 'signed')

    df = data_file.iloc[:,0:9]
    df.fillna(0.0)
    df.to_csv (os.path.join(csv_dir, day + ".csv"), index = False, header = False)

# Insert Data Into Target Database
def copyAdobeIntoPsql(yesterday):
    f = open(os.path.join(csv_dir, yesterday + ".csv"))
    conn = psycopg2.connect(
            host = "localhost",
            database = "suppliers",
            user = "postgres",
            password = "123456")
    cursor = conn.cursor()
    cursor.copy_from(f,'test_swap', sep = ',')
    conn.commit()
    conn.close()

# Main Function
if __name__ == '__main__':
    processes = []
    cores = os.cpu_count()

    for n in range(cores):
        process_x = Process(target = pullDataFromAdobe, args = (n,))
        processes.append(process_x)

    for process_x in processes:
        process_x.start()

    for process_x in processes:
        process_x.join()
