import psycopg2
import pandas as pd
import pyarrow.parquet as pq
import os
import sys
import requests
import time

from requests.exceptions import ConnectionError

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def GenerateAutoApprovalParquetsFromDbData(outputDir):
    conn = None
    try:
        conn = psycopg2.connect(user="postgres",
                                password="postgres",
                                host="localhost",
                                port="5432",
                                database="postgres")


        df = pd.read_sql(f"select * from orderheader;", conn)
        print("outputdir = ", outputDir)
        df = df.astype({"precisionbuildsw": int, "ordergroupbuildrule": int, "precisionloadsw": int,
                        "vehicleloadcount": int, "keepuseradjsw": int, "optimizforcedsw": int,
                        "systemapprovedsw": int})

        for x in df.select_dtypes(include=['datetime64']).columns.tolist():
            df[x] = df[x].dt.strftime('%Y-%m-%d %H:%M:%S')
        #df['orderplacedate'] = df['orderplacedate'].astype(str)
        #df['orderplacedate'] = df['orderplacedate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df.to_parquet(outputDir + '/OrderHeader.parquet', engine='pyarrow', compression='snappy')


        df1 = pd.read_sql(f"select * from vehicleload;", conn)
        print("data frame = ", df1)
        df1 = df1.astype({"sourcestatus": int, "deststatus": int, "lbstatus": int,
                        "lbsource": int, "vendorminmetsw": int, "transmodeminmetsw": int,
                        "tolerancecapmetsw": int, "maxcapacityexceededsw": int, "maxcapacitymetsw": int,
                          "approvalstatus": int})

        for x in df1.select_dtypes(include=['datetime64']).columns.tolist():
            df1[x] = df1[x].dt.strftime('%Y-%m-%d %H:%M:%S')
        # df['orderplacedate'] = df['orderplacedate'].astype(str)
        # df['orderplacedate'] = df['orderplacedate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df1.to_parquet(outputDir + '/VehicleLoad.parquet', engine='pyarrow', compression='snappy')


        df2 = pd.read_sql(f"select * from OrderException;", conn)
        for x in df2.select_dtypes(include=['datetime64']).columns.tolist():
            df2[x] = df2[x].dt.strftime('%Y-%m-%d %H:%M:%S')
        # df['orderplacedate'] = df['orderplacedate'].astype(str)
        # df['orderplacedate'] = df['orderplacedate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df2.to_parquet(outputDir + '/OrderException.parquet', engine='pyarrow', compression='snappy')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def truncateDbTables():
    conn1 = None
    try:
        conn1 = psycopg2.connect(user="postgres",
                                password="postgres",
                                host="localhost",
                                port="5432",
                                database="postgres")

        print(conn1)
        cur = conn1.cursor()
        username = "postgres"
        cur.execute(f"SELECT truncate_tables('{username}');")
        conn1.commit()
        print("Truncated db tables")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn1 is not None:
            conn1.close()



def AutoApproval():
    service_endpoint_url = 'http://localhost:8080/test_dcro_engine_service/trigger'
    testCasesStringList = []
    current_dir = os.getcwd()
    batch_cases_file_path = current_dir + "/batchcasesForStpParquetGenerateForLTP"
    total_cases = file_len(batch_cases_file_path)
    print("Total cases =", total_cases)
    test_case_outputDir = current_dir + "/dcroengineoutput"

    with open(batch_cases_file_path) as file:
        for testCaseLine in file:
            testCaseLine = testCaseLine.strip()
            testCasesStringList.append(testCaseLine)

    for testCaseString in testCasesStringList:
        testCaseData = testCaseString.split(":")
        testCaseName = testCaseData[0]
        orderPlaceDate = testCaseData[1]
        fallbackOrderDays = int(testCaseData[2])
        print("testcases =", testCaseData)
        result1 = executeTestCase(testCaseName, orderPlaceDate, fallbackOrderDays, service_endpoint_url, "false")
        outputDir = test_case_outputDir + "/" + testCaseName

        print("output dir = ", outputDir, "ordrplacedate = ", orderPlaceDate)
        GenerateAutoApprovalParquetsFromDbData(outputDir)
        time.sleep(2)
        truncateDbTables()

    print("done...")


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def executeTestCase(testCaseName, orderPlaceDate, fallbackOrderDays, service_endpoint_url, isLongTermProjections):
    response = 'executed'
    inputdata = {'orderPlaceDate': orderPlaceDate, 'fallback_order_days': fallbackOrderDays,
                 'isLongTermProjections': isLongTermProjections}
    try:
        res = requests.post(service_endpoint_url + "?inputFolderName=" + testCaseName, json=inputdata)
        if (res.status_code != 200):
            response = 'error'
    except ConnectionError as e:  # This is the correct syntax
        print("Looks like service is Down !!")
        print("Exiting script now !!")
        sys.exit(1)
    time.sleep(.05)
    return response


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    AutoApproval()
