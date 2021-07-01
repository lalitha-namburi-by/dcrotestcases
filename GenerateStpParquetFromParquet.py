import psycopg2
import pandas as pd
import pyarrow.parquet as pq
import os
import sys
import requests
import time

from pyspark.python.pyspark.shell import spark
from requests.exceptions import ConnectionError
from datetime import datetime
from datetime import timedelta


def generateStpParquetFromParquet(inputDir, outputDir, orderplacedate, stpenddate):
    orderheader_parquet = spark.read.parquet(outputDir + "/lrr_proj_orderheader.parquet")
    ordersku_parquet = spark.read.parquet(outputDir + "/lrr_proj_ordersku.parquet")

    orderheader_parquet.createOrReplaceTempView("orderheader_parquet")
    ordersku_parquet.createOrReplaceTempView("ordersku_parquet")

    df = spark.sql(f"SELECT os.item AS itemid,os.dest AS destination,os.soq AS soq,os.arrivdate AS arrivedate,"
                          f"os.ordergroup AS ordergroup,os.orderpointdate as orderpointdate,os.orderpointprojoh as "
                          f"orderpointprohoh,os.orderpointssqty as orderpointssqty,os.source as source,"
                          f"oh.orderplacedate AS orderplacedate FROM ordersku_parquet os, ( SELECT * FROM ( SELECT "
                          f"ordergroup,orderid,orderplacedate,createdate FROM orderheader_parquet WHERE orderid = "
                          f"grouporderid) lpoh1 INNER JOIN ( SELECT lpohA.og AS og,lpohA.cd AS cd,max(lpohA.opd) AS "
                          f"opd FROM ( SELECT ordergroup AS og, orderplacedate AS opd, createdate AS cd FROM "
                          f"orderheader_parquet WHERE orderid = grouporderid AND projectiontype = 1 AND DATE("
                          f"orderplacedate) > '{orderplacedate}' AND DATE(orderplacedate) < '{stpenddate}') lpohA "
                          f"INNER JOIN ( SELECT ordergroup AS og, max(createdate) AS cd FROM orderheader_parquet "
                          f"WHERE orderid = grouporderid AND projectiontype = 1 AND DATE(orderplacedate) > '"
                          f"{orderplacedate}' AND DATE(orderplacedate) < '{stpenddate}' GROUP BY ordergroup) lpohB ON "
                          f"lpohA.og = lpohB.og AND lpohA.cd = lpohB.cd GROUP BY lpohA.og, lpohA.cd) lpoh2 ON "
                          f"lpoh1.ordergroup = lpoh2.og AND lpoh1.orderplacedate = lpoh2.opd AND lpoh1.createdate = "
                          f"lpoh2.cd) oh WHERE os.grouporderid = oh.orderid AND os.ordergroup = oh.ordergroup;")

    df.repartition(1).write.parquet(inputDir + '/latest-short-term-order-projections.parquet', "overwrite", compression='snappy')


def generateStpParquetsForTestCases():
    service_endpoint_url = 'http://localhost:8080/test_dcro_engine_service/trigger'
    testCasesStringList = []
    current_dir = os.getcwd()
    batch_cases_file_path = current_dir + "/batchcasesForStpParquetGenerateForLTP"
    total_cases = file_len(batch_cases_file_path)
    print("Total cases =", total_cases)
    test_case_inputdir = current_dir + "/dcroengineinput"
    test_case_outputdir = current_dir + "/dcroengineoutput"

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
        inputDir = test_case_inputdir + "/" + testCaseName
        outputDir = test_case_outputdir + "/" + testCaseName

        Begindate = datetime.strptime(orderPlaceDate, "%Y-%m-%d")
        stpEndDate = Begindate + timedelta(days=14)
        print("end date =", stpEndDate)
        print("input dir = ", inputDir, "output dir = ", outputDir, "ordrplacedate = ", orderPlaceDate, "stpenddate = ",
              stpEndDate)
        generateStpParquetFromParquet(inputDir, outputDir, orderPlaceDate, stpEndDate)

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
