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


def GenerateStpParquetFromDbData(outputDir, orderplacedate, stpenddate):
    conn = None
    try:
        conn = psycopg2.connect(user="1024203",
                                password="majorram",
                                host="localhost",
                                port="5432",
                                database="ltpstp")


        df = pd.read_sql_query(
            f"SELECT os.item AS itemid,os.dest AS destination,os.soq AS soq,os.arrivdate AS arrivedate,os.ordergroup AS ordergroup,os.orderpointdate as orderpointdate,os.orderpointprojoh as orderpointprohoh,os.orderpointssqty as orderpointssqty,os.source as source,oh.orderplacedate AS orderplacedate FROM lrr_proj_ordersku os, ( SELECT * FROM ( SELECT ordergroup,orderid,orderplacedate,createdate FROM lrr_proj_orderheader WHERE orderid = grouporderid) lpoh1 INNER JOIN ( SELECT lpohA.og AS og,lpohA.cd AS cd,max(lpohA.opd) AS opd FROM ( SELECT ordergroup AS og, orderplacedate AS opd, createdate AS cd FROM lrr_proj_orderheader WHERE orderid = grouporderid AND projectiontype = 1 AND DATE(orderplacedate) > '{orderplacedate}' AND DATE(orderplacedate) < '{stpenddate}') lpohA INNER JOIN ( SELECT ordergroup AS og, max(createdate) AS cd FROM lrr_proj_orderheader WHERE orderid = grouporderid AND projectiontype = 1 AND DATE(orderplacedate) > '{orderplacedate}' AND DATE(orderplacedate) < '{stpenddate}' GROUP BY ordergroup) lpohB ON lpohA.og = lpohB.og AND lpohA.cd = lpohB.cd GROUP BY lpohA.og, lpohA.cd) lpoh2 ON lpoh1.ordergroup = lpoh2.og AND lpoh1.orderplacedate = lpoh2.opd AND lpoh1.createdate = lpoh2.cd) oh WHERE os.grouporderid = oh.orderid AND os.ordergroup = oh.ordergroup;",
            conn)
        print("data frame = ", df)
        print("outputdir = ", outputDir)
        df.to_parquet(outputDir + '/latest-short-term-order-projections.parquet', compression='snappy')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def truncateDbTables():
    conn1 = None
    try:
        conn1 = psycopg2.connect(user="1024203",
                                password="majorram",
                                host="localhost",
                                port="5432",
                                database="ltpstp")

        print(conn1)
        cur = conn1.cursor()
        username = "1024203"
        cur.execute(f"SELECT truncate_tables('{username}');")
        conn1.commit()
        print("Truncated db tables")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn1 is not None:
            conn1.close()



def generateStpParquetsForTestCases():
    service_endpoint_url = 'http://localhost:8080/test_dcro_engine_service/trigger'
    testCasesStringList = []
    current_dir = os.getcwd()
    batch_cases_file_path = current_dir + "/batchcases"
    total_cases = file_len(batch_cases_file_path)
    print("Total cases =", total_cases)
    test_case_inputdir = current_dir + "/dcroengineinput"

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
        outputDir = test_case_inputdir + "/" + testCaseName
        stpEndDate = '2021-02-15'
        if orderPlaceDate == '2006-10-30':
            stpEndDate = '2006-11-13'
        elif orderPlaceDate == '2021-02-03':
            stpEndDate = '2021-02-17'

        print("output dir = ", outputDir, "ordrplacedate = ", orderPlaceDate, "stpenddate = ", stpEndDate)
        GenerateStpParquetFromDbData(outputDir, orderPlaceDate, stpEndDate)
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
    generateStpParquetsForTestCases()
