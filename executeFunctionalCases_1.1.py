#!/usr/bin/env python3
import pyarrow.parquet as pq
import pandas as pd
import os
import sys
import requests
import datetime
import time

from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType, LongType
from requests.exceptions import ConnectionError
from datetime import datetime
from datetime import timedelta

#This function prints text in red color on terminal.
def print_red(text):
    print('\033[31m', text, '\033[0m', sep='') 

#This function prints text in green color on terminal.
def print_green(text):
    print('\033[32m', text, '\033[0m', sep='')

#This function prints text in yellow color on terminal.
def print_yellow(text):
    print('\033[33m', text, '\033[0m', sep='')

#This function prints text in blue color on terminal.
def print_blue(text):
    print('\033[34m', text, '\033[0m', sep='')

#This function creates the directory if it does not exist.
def createDirectory( dirName):   
    if not os.path.exists(dirName):
        os.makedirs(dirName)
    return;

def generateStpParquetFromParquet(inputDir, outputDir, orderplacedate, stpenddate):
    spark = SparkSession \
        .builder \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .appName("dcrotestcases") \
        .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    orderheader_parquet = spark.read.parquet(outputDir + "/lrr_proj_orderheader.parquet")
    ordersku_parquet = spark.read.parquet(outputDir + "/lrr_proj_ordersku.parquet")

    orderheader_parquet.createOrReplaceTempView("orderheader_parquet")
    ordersku_parquet.createOrReplaceTempView("ordersku_parquet")

    df = spark.sql(f"SELECT os.item AS item,os.dest AS dest,os.soq AS soq,os.arrivdate AS arrivdate,"
                   f"os.ordergroup AS ordergroup,os.orderpointdate as orderpointdate,os.orderpointprojoh as "
                   f"orderpointprojoh,os.orderpointssqty as orderpointssqty,os.source as source,"
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

    df = df.withColumn("arrivdate", col("arrivdate").cast(TimestampType()).cast(LongType()) * 1000)
    df = df.withColumn("orderplacedate", col("orderplacedate").cast(TimestampType()).cast(LongType()) * 1000)
    df = df.withColumn("orderpointdate", col("orderpointdate").cast(TimestampType()).cast(LongType()) * 1000)

    df.show()
    df.repartition(1).write.parquet(inputDir + '/latest_short_term_order_projections.parquet', "overwrite",
                                    compression='snappy')


#This Function executes a testcase by triggering the service
def executeTestCase(testCaseName,orderPlaceDate,fallbackOrderDays,service_endpoint_url, isLongTermProjections):
    response = 'executed'
    print_green("Executing " + testCaseName)
    inputdata = {'orderPlaceDate':orderPlaceDate,'fallback_order_days':fallbackOrderDays,'isLongTermProjections':isLongTermProjections}
    try:
        res = requests.post(service_endpoint_url+"?inputFolderName="+testCaseName, json =inputdata)
        if(res.status_code != 200):
            response ='error'
    except ConnectionError as e:    # This is the correct syntax
            print_red("Looks like service is Down !!")
            print_red("Exiting script now !!")
            sys.exit(1)
    time.sleep(.05)
    return response

#This function tells us the number of lines in a file.
def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

#This functions prints the percentage of executed testcases on terminal.
def printPercentage(executed_cases,total_cases):
    percentage = (100*executed_cases)/total_cases
    #cursor up one line
    sys.stdout.write('\x1b[1A')
    #delete last line
    sys.stdout.write('\x1b[2K')
    print_green("Executed "+ str(int(percentage))+" % testcases")

#This function compares the output data of a testcase with baseline results. 
def compare_output(baselines_dir,output_dir,testCaseName,fileandcolumnnames,fileandsortcolumnnames,files_with_differences,output_text_file_dir):
    testcase_baselines_dir = baselines_dir + testCaseName +"/"
    baseline_files_and_dirs = os.listdir(testcase_baselines_dir)
    inner_folders = [f.name for f in os.scandir(testcase_baselines_dir) if f.is_dir()]
    files_in_current_folder = [i for i in baseline_files_and_dirs if i not in inner_folders]
    testcase_output_dir = output_dir + testCaseName +"/"
    isPassed = compare_files(testcase_baselines_dir,testcase_output_dir,files_in_current_folder,fileandcolumnnames,fileandsortcolumnnames,files_with_differences,output_text_file_dir)

    for inner_folder in inner_folders:
        update_output_text_file_dir = output_text_file_dir + inner_folder + "/"
        append_text_to_consolidated_file(consolidated_output_file_path,inner_folder)
        append_text_to_consolidated_file(consolidated_baseline_file_path,inner_folder)
        
        createDirectory(update_output_text_file_dir)
        isEqual = compare_output(testcase_baselines_dir,testcase_output_dir,inner_folder,fileandcolumnnames,fileandsortcolumnnames,files_with_differences,update_output_text_file_dir)
        if(isEqual != 'success'):
            isPassed = 'failure'

    return isPassed

#This function writes parquet data to a text file.
def write_parquet_to_textfile(dir_path,file_name,parquet_data,suffix):
    textfilepath=dir_path+file_name+suffix
    text_file = open(textfilepath, 'w')
    text_file.write(parquet_data.to_string())
    text_file.close()

#This functions write text to consolidated outbut and baseline text files.
def append_text_to_consolidated_file(file_path,text):
    consolidated_file = open(file_path, 'a')
    consolidated_file.write(text)
    consolidated_file.write('\n')
    consolidated_file.close()

#This function compares the output parquet files of a testcase against the baseline parquet files.
def compare_files(testcase_baselines_dir,testcase_output_dir,files_to_compare,fileandcolumnnames,fileandsortcolumnnames,files_with_differences,output_text_file_dir):
    isEqual = 'success'
    for file in files_to_compare:
        file_data = file.split(".")
        file_name = file_data[0].lower()

        if fileandcolumnnames.get(file_name) == None:
            print_yellow("skipping comparison of file : "+file_name)
            print()
            continue
        columnnames=fileandcolumnnames[file_name]
        sortcolumnnames=fileandsortcolumnnames[file_name]
        baseline_file = testcase_baselines_dir+file
        output_file = testcase_output_dir+file

        
        baselineparquet = pd.read_parquet(baseline_file,columns=columnnames)
        baselineparquet.sort_values(by=sortcolumnnames, inplace=True)
        baselineparquet.reset_index(drop=True, inplace=True)

        write_parquet_to_textfile(output_text_file_dir,file_name,baselineparquet,"baseline.txt")
        append_text_to_consolidated_file(consolidated_baseline_file_path,file_name)
        append_text_to_consolidated_file(consolidated_baseline_file_path,baselineparquet.to_string())

        outputparquet=pd.read_parquet(output_file,columns=columnnames)
        outputparquet.sort_values(by=sortcolumnnames, inplace=True)
        outputparquet.reset_index(drop=True, inplace=True)
        
        write_parquet_to_textfile(output_text_file_dir,file_name,outputparquet,"output.txt")
        append_text_to_consolidated_file(consolidated_output_file_path,file_name)
        append_text_to_consolidated_file(consolidated_output_file_path,outputparquet.to_string())
        
        
        if(not(outputparquet.equals(baselineparquet))):
            files_with_differences.append(output_file)
            isEqual ='failure'
    
    return isEqual

#This function generates a HTML report for the batch run.
def generateHTMLReport(report_name,results_path,files_with_differences_dict,executed_cases_list,error_cases_list):
    total_cases_count = len(executed_cases_list)
    error_cases_count = len(error_cases_list)
    failed_cases_count = len(files_with_differences_dict)
    passed_cases_count = total_cases_count - (error_cases_count+failed_cases_count)
    report_path=bacthrun_results_path+"batchcasesreport.html"

    message="""<html>
    <head>
    <style>
        table, th, td {
        border: 1px solid black;
        }
    </style>
    </head>
    <body>
    """

    message += "<table align='center'>"
    
    message += "<tr>"
    message += "<th> Report ID </th>"
    message += "<th> Total Case </th>"
    message += "<th bgcolor=lime> Passed Cases </th>"
    message += "<th bgcolor=yellow> Cases with Error </th>"
    message += "<th bgcolor=red> Failed Cases </th>"
    message += "</tr>"

    message += "<tr>"
    message += "<td>"+report_name+"</td>"
    message += "<td>"+str(total_cases_count)+"</td>"
    message += "<td>"+str(passed_cases_count)+"</td>"
    message += "<td>"+str(error_cases_count)+"</td>"
    message += "<td>"+str(failed_cases_count)+"</td>"
    message += "</tr>"
    
    message+="</table>"

    message += "<br/>"
    message += "<br/>"

    if error_cases_count != 0:
        #creating tables for cases with error
        string_of_cases_with_error = ""

        for error_case in error_cases_list:
            string_of_cases_with_error += error_case
            string_of_cases_with_error += " , "

        message += "<table>"
    
        message += "<tr>"
        message += "<td>"+"Testcases with Error"+"</td>"
        message += "<td>"+string_of_cases_with_error+"</td>"
        message += "</tr>"

        message+="</table>"

        message += "<br/>"
        message += "<br/>"

   
    if failed_cases_count != 0:
        #creating table for failed cases
        message += "<table>"
        message += "<tr>"
        message += "<th>"+"Failed Case"+"</th>"
        message += "<th>"+"Files with differences"+"</th>"
        message += "</tr>"

        for failed_case in files_with_differences_dict:
            message += "<tr>"
            message += "<td>"+failed_case+"</td>"
            message += "<td>"+str(files_with_differences_dict[failed_case])+"</td>"
            message += "</tr>"

        message += "</table>"

        message += "<br/>"
        message += "<br/>"


    #creating for all executed cases
    string_of_all_executed_cases = ""

    for executed_case in executed_cases_list:
        string_of_all_executed_cases += executed_case
        string_of_all_executed_cases += " , "

    message += "<table>"
    
    message += "<tr>"
    message += "<td>"+"Executed Cases"+"</td>"
    message += "<td>"+string_of_all_executed_cases+"</td>"
    message += "</tr>"

    message+="</table>"

    message+="""
    </body>
    </html>"""

    f = open(report_path,'w')
    
    f.write(message)
    f.close()
    print("Please Find the Report Here : ")
    print_blue(report_path)
    return;

#service endpoint URL
service_endpoint_url = 'http://localhost:8080/test_dcro_engine_service/trigger'

#fetch the current directory path where our script resides
current_dir = os.getcwd()

#directory where all input parquet files stored
input_dir = current_dir+"/dcroengineinput"

#directory where all the baseline parquet files resides
baselines_dir = current_dir+"/outputbaselines/"

#directory where all the output files of testcase gets generated
output_dir = current_dir+"/dcroengineoutput/"

#directory where report fo batch run will be generated
results_path = current_dir+"/testresults/"

#use report name if it is provided when script is triggered otherwise we will
#use the current date and time to generate a default report name
report_name=''
try:
    report_name=sys.argv[1]
except IndexError as e:
    print("No Batch run id provided Using Default")
    current_time = datetime.now()
    report_name="Report-"+current_time.strftime("%m-%d-%Y-%H-%M-%S")

bacthrun_results_path=results_path+report_name+"/"

#path to the consolidated output text file
consolidated_output_file_path = bacthrun_results_path+"consolidatedoutputfile.txt"

#path to the consolidated baseline text file
consolidated_baseline_file_path = bacthrun_results_path+"consolidatedbaselinefile.txt"

#create the directory for results
createDirectory(bacthrun_results_path);

batch_cases_file_path = current_dir + "/batchcases"

orderheadercolumns=["source","dest","transmode","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype","keepuseradjsw","optimizforcedsw"]
orderheadersortcolumns=["source","dest","transmode","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype","keepuseradjsw","optimizforcedsw"]

projorderheadercolumns = orderheadercolumns.copy()
projorderheadercolumns.append("projectiontype")

projorderheadersortcolumns = orderheadersortcolumns.copy()
projorderheadersortcolumns.append("projectiontype")

orderexceptioncolumns = ["exception","exceptiondate","descr","item","source","dest","transmode","ordergroup","ordergroupmember"]
orderexceptionsortcolumns=["exception","exceptiondate","descr","item","source","dest","transmode","ordergroup","ordergroupmember"]

orderskucolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","usersoqoverride"]
orderskusortcolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","usersoqoverride"]

orderskudetailcolumns=["item","dest","departuredate","deliverydate","totalleadtime","loaddur","transitdur","unloaddur","adjskucovdur","avgreplenqty","stocklowdate","stocklowdur","stocklowqty","stockoutdate","stockoutdur","stockoutqty","arrivcovdur","sysorderpointdate","sysorderuptoleveldate","precisionbuildsw","precisionloadsw"]
orderskudetailsortcolumns=["item","dest","departuredate","deliverydate","totalleadtime","loaddur","transitdur","unloaddur","adjskucovdur","avgreplenqty","stocklowdate","stocklowdur","stocklowqty","stockoutdate","stockoutdur","stockoutqty","arrivcovdur","sysorderpointdate","sysorderuptoleveldate","precisionbuildsw","precisionloadsw"]

orderskutotalcolumns=["item","dest","uom","qty","unroundedqty"]
orderskutotalsortcolumns=["item","dest","uom","qty","unroundedqty"]

ordertotalcolumns=["type","uom", "qty","unroundedqty"]
ordertotalsortcolumns=["type","uom", "qty","unroundedqty"]

vehicleloadcolumns=["transmode","shipdate","arrivdate","sourcestatus","deststatus","lbstatus","lbsource","transmodeminmetsw","tolerancecapmetsw","maxcapacitymetsw","vendorminmetsw","orderoptseqnum","maxcapacityexceededsw","approvalstatus"]
vehicleloadsortcolumns=["transmode","shipdate","arrivdate","sourcestatus","deststatus","lbstatus","lbsource","transmodeminmetsw","tolerancecapmetsw","maxcapacitymetsw","vendorminmetsw","orderoptseqnum","maxcapacityexceededsw","approvalstatus"]

vehicleloadlinecolumns=["item","primaryitem","qty","schedshipdate","schedarrivdate","expdate","lbsource","source","dest"]
vehicleloadlinesortcolumns=["item","primaryitem","qty","schedshipdate","schedarrivdate","expdate","lbsource","source","dest"]

vehicleloadtotalcolumns=[ "uom","qty"]
vehicleloadtotalsortcolumns=["uom","qty"]

orderheaderhistcolumns=["source","dest","transmode","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype","keepuseradjsw","optimizforcedsw"]
orderheaderhistsortcolumns=["source","dest","transmode","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype","keepuseradjsw","optimizforcedsw"]

ordertotalhistcolumns=["type","uom", "qty","unroundedqty"]
ordertotalhistsortcolumns=["type","uom", "qty","unroundedqty"]

fileandcolumnnames = {"orderheader": orderheadercolumns, "orderexception": orderexceptioncolumns, "ordersku": orderskucolumns, "orderskudetail": orderskudetailcolumns, "orderskutotal": orderskutotalcolumns,"ordertotal":ordertotalcolumns,"vehicleload":vehicleloadcolumns,"vehicleloadline":vehicleloadlinecolumns,"vehicleloadtotal":vehicleloadtotalcolumns,"orderheaderhist":orderheaderhistcolumns,"ordertotalhist":ordertotalhistcolumns,"lrr_proj_orderheader": projorderheadercolumns, "lrr_proj_orderexception": orderexceptioncolumns, "lrr_proj_ordersku": orderskucolumns, "lrr_proj_orderskudetail": orderskudetailcolumns, "lrr_proj_orderskutotal": orderskutotalcolumns,"lrr_proj_ordertotal":ordertotalcolumns,"lrr_proj_vehicleload":vehicleloadcolumns,"lrr_proj_vehicleloadline":vehicleloadlinecolumns,"lrr_proj_vehicleloadtotal":vehicleloadtotalcolumns}

fileandsortcolumnnames={"orderheader": orderheadersortcolumns, "orderexception": orderexceptionsortcolumns, "ordersku": orderskusortcolumns, "orderskudetail": orderskudetailsortcolumns, "orderskutotal": orderskutotalsortcolumns,"ordertotal":ordertotalsortcolumns,"vehicleload":vehicleloadsortcolumns,"vehicleloadline":vehicleloadlinesortcolumns,"vehicleloadtotal":vehicleloadtotalsortcolumns,"orderheaderhist":orderheaderhistsortcolumns,"ordertotalhist":ordertotalhistsortcolumns,"lrr_proj_orderheader": projorderheadersortcolumns, "lrr_proj_orderexception": orderexceptionsortcolumns, "lrr_proj_ordersku": orderskusortcolumns, "lrr_proj_orderskudetail": orderskudetailsortcolumns, "lrr_proj_orderskutotal": orderskutotalsortcolumns,"lrr_proj_ordertotal":ordertotalsortcolumns,"lrr_proj_vehicleload":vehicleloadsortcolumns,"lrr_proj_vehicleloadline":vehicleloadlinesortcolumns,"lrr_proj_vehicleloadtotal":vehicleloadtotalsortcolumns}

total_cases = 0

files_with_differences_dict = {}

error_cases_list = []

executed_cases = 0

executed_cases_list = []

testCasesStringList = []

argsdata = sys.argv
argslen = len(argsdata)

if argslen > 2:
    testcaseList = argsdata[2]
    testCasesStringList = testcaseList.split(",")
    total_cases = len(testCasesStringList)
else :
    total_cases = file_len(batch_cases_file_path)
    with open(batch_cases_file_path) as file :
        for testCaseLine in file :
            testCaseLine = testCaseLine.strip()
            testCasesStringList.append(testCaseLine)

for testCaseString in testCasesStringList:
    testCaseData = testCaseString.split(":")
    testCaseName = testCaseData[0]
    orderPlaceDate = testCaseData[1]
    fallbackOrderDays = int(testCaseData[2])
    begindate = datetime.strptime(orderPlaceDate, "%Y-%m-%d")
    stpEndDate = begindate + timedelta(days=14)
    result1 = executeTestCase(testCaseName,orderPlaceDate,fallbackOrderDays,service_endpoint_url,"false")
    #generateStpParquetFromParquet((input_dir+"/"+testCaseName), (output_dir+"/"+testCaseName), orderPlaceDate,
                                  #stpEndDate)
    result2 = executeTestCase(testCaseName,orderPlaceDate,fallbackOrderDays,service_endpoint_url,"true")
    executed_cases = executed_cases + 1
    executed_cases_list.append(testCaseName)
    printPercentage(executed_cases,total_cases)
    if(result1 == 'error' or result2 == 'error'):
        error_cases_list.append(testCaseName)
        continue;
    files_with_differences = []
    output_text_file_dir = bacthrun_results_path + testCaseName + "/"
    createDirectory(output_text_file_dir)
    append_text_to_consolidated_file(consolidated_output_file_path,testCaseName)
    append_text_to_consolidated_file(consolidated_baseline_file_path,testCaseName)
    result = compare_output(baselines_dir,output_dir,testCaseName,fileandcolumnnames,fileandsortcolumnnames,files_with_differences,output_text_file_dir)
    if(result == 'failure'):
        files_with_differences_dict[testCaseName] = files_with_differences
 
generateHTMLReport(report_name,bacthrun_results_path,files_with_differences_dict,executed_cases_list,error_cases_list)

#Also give execute permission to createSummaryForAll.py on your localc machine
#Uncomment these lines to generate summary.
#script_command = "./createSummaryForAll.py"
#os.system(script_command)

