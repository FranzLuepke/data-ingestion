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


# STATE AN ADOBE ANALYTICS OAUTH CLIENT
#*************************************************************
ac = analytics_client(
    authClientId = os.getenv('CLIENT_ID'),
    clientSecret = os.getenv('CLIENT_SECRET'),
    accountId = os.getenv('GLOBAL_COMPANY_ID')
)
#END: STATE AN ADOBE ANALYTICS OAUTH CLIENT
#*************************************************************

# PULL DATA FROM ADOBE ANALYTICS
# **** FUNCTION, HAS 1 ARGUMENT, NUMBER OF DAYS BEHIND TODAY'S DATE TO PULL OUT data
# REQUIRES: OS, DATETIME, TIMEDELTA, DATE, AND ANALYTICS.MAYHEM.ADOBE
#----------------------------------------------------------------------------------------------------
#Step 1: authenticate -- BE MINDFUL, THIS CODE USES OAUTH2, WITHOUT SELENOUM webdriver
# or alternative automation solution that provides well handling of Adobe IMS you
# won't be able to fully automate this.
# Automation requires JWT ClientID, ClientSecret, adobe tool parameters and a private key file
# go to adobe.io/console as admin to get one, inside RCCL reach out Web analytics admin team.
#**************************************************************************************************
#Step 2: set the target RSID - report suite id -  implementation focused on virtual rsid # 5 at RCCI1
#**************************************************************************************************
#Step 3: Add required metrics, no particular order is needed
#**************************************************************************************************
#Step 4: add required dimensions,
######## WARNING: PARTICULAR ORDER AFFECTS Performance!!
########  https://experienceleague.adobe.com/docs/analytics/analyze/analysis-workspace/workspace-faq/optimizing-performance.html?lang=en
######## Use the biggest span initially, then the shortest to biggest.
#Step 5: get system date and substract a number of days (days behind) to pull out data
######## This implementation is focused on pulling out DAILY data by each instantiation
######## you can modify this if required, but be mindful that this affets performance,
######## if you need to pull more than one day, add by the end, a dimension called:
######## add_dimension(dimension_name = 'variables/daterangeday')

#Step 6: Set date range with ** set_date_range ** fx from analytics mayhem lib.
#**************************************************************************************************
#Step 7: run the get report or get report multiple breakdowns, this implementation uses the 2nd
#**************************************************************************************************
#Step 8: do some formatting to the dataframe to match target database schema easily
######## target db is a psql standalon implementation, powerbiswap is the db, rcci_aa_swap the table.
#**************************************************************************************************
#Step 9: Store the data pulled out into a pandas dataframe into csv for faster import into psql
######## psycopg2 library allows the use of COPY command which makes the psql load very efficient.
######## the file is stored using YYYY-MM-DD.csv FORMAT!

##############################################
### THAT IS ALL ABOUT PULL OUT DATA FROM ADOBE

def pullDataFromAdobe(daysbehind):
    ad._authenticate()
    ad.set_report_suite(report_suite_id = os.getenv('VIRTUAL_REPORT_SUITE_ID'))
    ad.add_global_segment(segment_id = "s300006910_5deadb4233d1221935ab2b9e") # RCI - Non booked Guests 2020 Final -- Segment
    ad.add_metric(metric_name= 'metrics/visits')
    ad.add_metric(metric_name= 'cm300006910_58cac16ebd01054f2476fe89') # CONV
    ad.add_metric(metric_name= 'cm300006910_59833205a5bb573a7947f382') # FUNNEL ENTRIES
    ad.add_metric(metric_name= 'cm300006910_5983275d985eebe77e215664') # CONF PAGE VISITS
    # setting requested dimensions
    ad.add_dimension(dimension_name = 'variables/geodma') # US DMA
    ad.add_dimension(dimension_name = 'variables/product.4') # metaprod
    ad.add_dimension(dimension_name = 'variables/evar14.2') # Quarter

    #delete_buffer()
    #auth method on aouth2 - check with Brandon JWT data..

    # setting report suite ID
    #getting yesterday
    yesterday = datetime.strftime(date.today()- timedelta(days=daysbehind), '%Y-%m-%d')
    print(yesterday)

    # setting date range
    ad.set_date_range(date_start = yesterday, date_end= yesterday)
    #setting requested metrics pack

    #pulls out and stores into a dataframe - a pandas df
    data = ad.get_report_multiple_breakdowns()
    #DOING SOME FORMATING
    #**************************************************************************************************
    yesterdayxx = date.today()- timedelta(days=daysbehind)
    print(yesterdayxx)
    data_file = data.assign(month=yesterdayxx.month)
    data_file = data_file.assign(day=yesterdayxx.day)
    titles = list(data_file.columns)
    data_file = data_file[titles]

    df2=data_file[data_file.columns[[10,11,1,3,5,7,9,8,6,0,2,4]]]
    df2[["metrics/visits", "cm300006910_59833205a5bb573a7947f382", "cm300006910_5983275d985eebe77e215664"]] = df2[["metrics/visits", "cm300006910_59833205a5bb573a7947f382", "cm300006910_5983275d985eebe77e215664"]].apply(pd.to_numeric, errors='coerce', downcast='signed')
    df2[["cm300006910_58cac16ebd01054f2476fe89"]] =df2[["cm300006910_58cac16ebd01054f2476fe89"]].apply(pd.to_numeric, errors='coerce')

    df3=df2.iloc[:,0:9]
    df3.fillna(0.0)
    #*****ENDING OF FORMATTING ************************************************************************
    #**************************************************************************************************
    #*****STORING THE DATA INTO A CSV******************************************************************
    df3.to_csv (os.path.join(csv_dir, yesterday + ".csv"), index = False, header=False)
    #*****END OF CSV EXPORT ***************************************************************************

#*************************************************************
#END: PULL DATA FROM ADOBE ANALYTICS
#*************************************************************

#*************************************************************
# (RAPIDLY) INSERT DATA INTO TARGET DATABASE
# **** FUNCTION, HAS 1 ARGUMENT, Date in YYYY-MM-DD
##### fashion, this has to be string type.
##### used to search in same path the file stored by PART III
# REQUIRES: psycopg2, OS
# ------------------------------------------------------------
#Step1: Connects to target database
#Step2: Run COPY command using copy_from method by psycopg2
#*************************************************************


def copyAdobeIntoPsql(yesterday):
    f = open(os.path.join(csv_dir, yesterday + ".csv"))
    conn = psycopg2.connect(
            host="localhost",
            database="powerbiswap",
                user="postgres",
            password="root")
    cursor = conn.cursor()
    cursor.copy_from(f,'rcci_aa_swap', sep=',')
    conn.commit()
    conn.close()
#*************************************************************
#END: (RAPIDLY) INSERT DATA INTO TARGET DATABASE
#*************************************************************

#*************************************************************
#*************MULTIPROCESSING BLOCK***************************
if __name__ == '__main__':
    processes = []
    cores = os.cpu_count()
    print('Exiting ', cores,' cores available')

    print('------------Instantiate Adobe_PSQ_standalon_MP_RCCL_V0.01')

    #loop to deliver processes into cores
    for n in range(cores):
        #instantiate the processes
        #assign execution function and any required parameter
            #note for unexperienced developers with python 3 multiprocessing: target= some function you want to execute,
            #if the function has parameters, you must pass them using args=([YOUR PARAMS],) -- it must end with comma and closing parentheses
        process_x=Process(target=pull_data_from_adobe, args=(n,))
        #append to processes list
        processes.append(process_x)

    print('-----Execute list of process_x into processes')

    for process_x in processes:
        process_x.start()

    print('-----Pipeline, Wait, or Join the processes')

    for process_x in processes:
        process_x.join()

    print('----- going back to initial execution!!! ')

#*************END OF MULTIPROCESSING BLOCK********************
#*************************************************************