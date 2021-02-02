#!/usr/bin/env python3
import pyarrow.parquet as pq
import pandas as pd
import os
import sys
import requests
import datetime
import time

def createDirectory( dirName):   
    if not os.path.exists(dirName):
        os.makedirs(dirName)
    return;
        #print("Directory " , dirName ,  " Created ")
    #else :    
        #print("Directory " , dirName ,  " already exists")   
    

def generateHTMLReport(report_id,bacthrun_results_path,result_dict,filenames,detailed_result_dict,result_colors):
    report_path=bacthrun_results_path+"testcasereport.html"

    total_cases=len(result_dict)
    f = open(report_path,'w')
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
    message += "Report ID : " + report_id
    message += "<br/>"
    passed = 0
    message += "Toatl TestCases = "+str(total_cases)

    message += "<br/>"
    message += "<table>"
    message += "<tr>"
    message += "<th> TestCase </th>"
    message += "<th> Result </th>"

    for filename in filenames:
        message += "<th>"
        message += filename
        message += "</th>"

    message += "<th>"
    message +=  "Testcase result folder"
    message += "</th>"
    message += "<th> JsonResponse </th>"

    message += "</tr>"
    for test_id in result_dict:
        # create the colored cell:
        color = result_colors[result_dict[test_id]]
        message += "<tr>"
        message += "<td>"+test_id+"</td>"
        message += "<td bgcolor='"+color+"'>"+result_dict[test_id]+"</td>"
        if(result_dict[test_id] == 'success'):
            passed=passed+1

        detailed_result = detailed_result_dict[test_id];
        for filename in filenames:
            message += "<td>"+detailed_result[filename]+"</td>"

        path = bacthrun_results_path+"/"+test_id+"/"
        message += "<td>"+path+"</td>"
        message += "<td>"+jsonResponse_dict[test_id]+"</td>"

        message += "</tr>"
    message+="</table>"
    message+="<br/>"
    message+="Passed Testcases = "+str(passed)
    message+="<br/>"
    message += "<br/>"
    message+="Failed Testcases = "+str(total_cases-passed)
    message+="""
    </body>
    </html>"""
    f.write(message)
    f.close()
    print("Please Find the Report Here "+ report_path)
    return;

def printPercentage(num,totalCount):
    percentage = (100*num)/totalCount
    #cursor up one line
    sys.stdout.write('\x1b[1A')

    #delete last line
    sys.stdout.write('\x1b[2K')
    print("Executed "+ str(int(percentage))+" % testcases")

batchrunid=''
try:
    batchrunid=sys.argv[1]
except IndexError as e:
    print("No Batch run id provided Using Default")
    current_time = datetime.datetime.now()
    batchrunid="Report-"+current_time.strftime("%m-%d-%Y-%H-%M-%S")


print(batchrunid)

#input_data_path="/Users/1022177/Desktop/PythonScripts/dcroengineinput/"
current_dir = os.getcwd()

output_data_path=current_dir+"/dcroengineoutput/"

baseline_data_path=current_dir+"/outputbaselines/"

results_path=current_dir+"/testresults/"

bacthrun_results_path=results_path+batchrunid+"/"

result_colors = {
        'success':      'lime',
        'failure':      'red',
        'error':        'yellow',
    }

#testcases=["DCRO-100.01", "DCRO-100.02", "DCRO-100.03","DCRO-100.04", "DCRO-100.05", "DCRO-100.06","DCRO-100.07", "DCRO-100.08", "DCRO-100.09","DCRO-100.10", "DCRO-100.11", "DCRO-100.12","DCRO-100.13", "DCRO-100.14", "DCRO-100.15","DCRO-100.16", "DCRO-100.17", "DCRO-100.18","DCRO-100.19", "DCRO-100.20", "DCRO-100.21","OOPT-GAA319.03", "OOPT-GAA319.07", "OOPT-GAA319.09","OOPT-GAA319.11", "OOPT-GAA319.13", "OOPT-GAA319.15", "OOPT-GAA319.17","OOPT-GEX334.01","OOPT-GEX334.02","OOPT-GEX334.03","OOPT-GEX334.04","OOPT-GEX334.05","OOPT-GEX334.09","OOPT-GEX334.10"]

testcasesjson=["OOPT-REC_10.01","OOPT-REC_10.02","OOPT-REC_10.03","OOPT-REC_10.04","OOPT-REC_10.05","OOPT-REC_10.06","OOPT-REC_10.07","OOPT-REC_10.08","OOPT-REC_10.09","OOPT-REC10.10","OOPT-REC10.11","OOPT-REC10.12","OOPT-REC10.13","OOPT-REC10.14","OOPT-REC10.15","OOPT-REC10.16","OOPT-REC10.17","OOPT-REC10.18","OOPT-REC10.19","OOPT-REC10.20","OOPT-REC10.21","OOPT-REC10.23","OOPT-REC10.24","OOPT-REC10.25","OOPT-REC10.30","OOPT-REC10.31","OOPT-REC10.32","OOPT-REC10.33","OOPT-REC10.34","OOPT-REC10.35","OOPT-REC10.36","OOPT-REC10.37","OOPT-REC10.38","OOPT-REC10.39","OOPT-REC10.40","OOPT-REC10.41","OOPT-REC10.42","OOPT-REC10.43","OOPT-REC10.44","OOPT-REC10.45","OOPT-REC10.46","OOPT-REC10.47","OOPT-REC10.48","OOPT-REC10.49","OOPT-REC10.50","OOPT-LBS419.06","OOPT-LBS419.07","OOPT-LBS419.08","OOPT-LBS419.09","OOPT-LBS419.10","OOPT-LBS419.11"]

orderheadercolumns=["source","dest","transmode","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype","keepuseradjsw","optimizforcedsw"]
orderheadersortcolumns=["source","dest","transmode","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype","keepuseradjsw","optimizforcedsw"]

orderexceptioncolumns = ["exception","exceptiondate","descr","item","source","dest","transmode","ordergroup","ordergroupmember"]
#orderexceptionsortcolumns=["source","dest","transmode","item","exception"]
orderexceptionsortcolumns=["exception","exceptiondate","descr","item","source","dest","transmode","ordergroup","ordergroupmember"]

orderskucolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","supporderqty","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","sourcing","usersoqoverride"]
orderskusortcolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","supporderqty","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","sourcing","usersoqoverride"]

orderskudetailcolumns=["item","dest","departuredate","deliverydate","totalleadtime","loaddur","transitdur","unloaddur","adjskucovdur","avgreplenqty","stocklowdate","stocklowdur","stocklowqty","stockoutdate","stockoutdur","stockoutqty","arrivcovdur","sysorderpointdate","sysorderuptoleveldate","precisionbuildsw","precisionloadsw"]
orderskudetailsortcolumns=["item","dest","departuredate","deliverydate","totalleadtime","loaddur","transitdur","unloaddur","adjskucovdur","avgreplenqty","stocklowdate","stocklowdur","stocklowqty","stockoutdate","stockoutdur","stockoutqty","arrivcovdur","sysorderpointdate","sysorderuptoleveldate","precisionbuildsw","precisionloadsw"]

orderskutotalcolumns=["item","dest","uom","qty","unroundedqty"]
orderskutotalsortcolumns=["item","dest","uom","qty","unroundedqty"]

ordertotalcolumns=["type","uom", "qty","unroundedqty"]
ordertotalsortcolumns=["type","uom", "qty","unroundedqty"]

vehicleloadcolumns=["transmode","shipdate","arrivdate","sourcestatus","deststatus","lbstatus","lbsource","transmodeminmetsw","tolerancecapmetsw","maxcapacitymetsw","vendorminmetsw","orderoptseqnum","maxcapacityexceededsw","approvalstatus"]
vehicleloadsortcolumns=["transmode","shipdate","arrivdate","sourcestatus","deststatus","lbstatus","lbsource","transmodeminmetsw","tolerancecapmetsw","maxcapacitymetsw","vendorminmetsw","orderoptseqnum","maxcapacityexceededsw","approvalstatus"]

vehicleloadlinecolumns=["item","primaryitem","qty","schedshipdate","schedarrivdate","expdate","lbsource","sourcing","source","dest"]
vehicleloadlinesortcolumns=["item","primaryitem","qty","schedshipdate","schedarrivdate","expdate","lbsource","sourcing","source","dest"]

vehicleloadtotalcolumns=[ "uom","qty"]
vehicleloadtotalsortcolumns=["uom","qty"]

fileandcolumnnames = {"orderheader": orderheadercolumns, "orderexception": orderexceptioncolumns, "ordersku": orderskucolumns, "orderskudetail": orderskudetailcolumns, "orderskutotal": orderskutotalcolumns,"ordertotal":ordertotalcolumns,"vehicleload":vehicleloadcolumns,"vehicleloadline":vehicleloadlinecolumns,"vehicleloadtotal":vehicleloadtotalcolumns}

fileandsortcolumnnames={"orderheader": orderheadersortcolumns, "orderexception": orderexceptionsortcolumns, "ordersku": orderskusortcolumns, "orderskudetail": orderskudetailsortcolumns, "orderskutotal": orderskutotalsortcolumns,"ordertotal":ordertotalsortcolumns,"vehicleload":vehicleloadsortcolumns,"vehicleloadline":vehicleloadlinesortcolumns,"vehicleloadtotal":vehicleloadtotalsortcolumns}

filenames=["orderexception","orderheader","ordersku","orderskudetail","orderskutotal","ordertotal","vehicleload","vehicleloadline","vehicleloadtotal"]

result_dict={}
detailed_result_dict={}
jsonResponse_dict={}

consolidatedoutputfile = bacthrun_results_path+"consolidatedoutputfile.txt"
consolidatedbaselinefile = bacthrun_results_path+"consolidatedbaselinefile.txt"
createDirectory(bacthrun_results_path)

cofile = open(consolidatedoutputfile, 'w')
cbfile = open(consolidatedbaselinefile, 'w')

print("executing testcases now !!")

total_cases = len(testcasesjson)
total_Jsoncases = 0

executedcases = 0
baseinputdir = current_dir + "/dcroengineinput"

argsdata = sys.argv
argslen = len(argsdata)

if argslen > 1:
 #testcasesjson.clear()
 commandlinedata = argsdata[1]
 testcasesjson = commandlinedata.split(",")

print("\n")
for testcase in testcasesjson:
  #print(testcase)
  testcase = testcase.strip()
  if(testcase == ""):
    continue;

  url = 'http://localhost:8080/test_dcro_engine_service/recalculate?reset=false&force_optimize=false'
  headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}

  testcasepath = baseinputdir + "/"+testcase

  testcasefileslist = os.listdir(testcasepath)
  for file in testcasefileslist:
    if file.endswith("json"):
       jsonfile = file
    
  if len(jsonfile) == 0:
    continue
    
  recalctestcasefile = testcasepath+"/"+jsonfile 
  #print(recalctestcasefile)
  #inputfoldername = jsonfile.replace('.json','')
  jsonfile = ""
  total_Jsoncases = total_Jsoncases + 1
    
  data1 = open(recalctestcasefile, 'rb')
  res = requests.post(url,data=data1,headers = headers)
  #print(x.text)
  
  isPassed='success';

  jsonResponse_dict[testcase]=res.text
  
  if(res.status_code != 200):
    result_dict[testcase]='error'
    #print(res.status_code)
    continue;
  
  cofile.write(testcase)
  cofile.write('\n')

  cbfile.write(testcase)
  cbfile.write('\n')

  testcase_result_dict={}
  for filename in filenames:
    #print(filename)
    cofile.write('\n')
    cofile.write(filename)
    cofile.write('\n')

    cbfile.write('\n')
    cbfile.write(filename)
    cbfile.write('\n')
    outputfile=output_data_path+testcase+"/"+filename+".parquet"

    baselinefile=baseline_data_path+testcase+"/"+filename+".parquet"

    #parquet1 = pq.read_table(outputfile)
    #parquet2 = pq.read_table(baselinefile)
    #print(parquet1.equals(parquet2))
    
    columnnames=fileandcolumnnames[filename]
    sortcolumnnames=fileandsortcolumnnames[filename]

    outputparquet=pd.read_parquet(outputfile,columns=columnnames)
    outputparquet.sort_values(by=sortcolumnnames, inplace=True)
    outputparquet.reset_index(drop=True, inplace=True)
    #print(outputparquet)
    outputtextfiledir=bacthrun_results_path+testcase+"/"
    createDirectory(outputtextfiledir)
    outputtextfilepath=outputtextfiledir+filename+"output.txt"
    tfile = open(outputtextfilepath, 'w')
    cofile.write(outputparquet.to_string())
    tfile.write(outputparquet.to_string())
    tfile.close()
    #outputcsvfilepath=bacthrun_results_path+testcase+"/"+filename+"output.csv"
    #outputparquet.to_csv('outputcsvfilepath.csv') 
    
    baselineparquet = pd.read_parquet(baselinefile,columns=columnnames)
    baselineparquet.sort_values(by=sortcolumnnames, inplace=True)
    baselineparquet.reset_index(drop=True, inplace=True)
    #print(baselineparquet)
    baselinetextfiledir=bacthrun_results_path+testcase+"/"
    createDirectory(baselinetextfiledir)
    baselinetextfilepath=bacthrun_results_path+testcase+"/"+filename+"baseline.txt"
    tfile = open(baselinetextfilepath, 'w')
    tfile.write(baselineparquet.to_string())
    cbfile.write(baselineparquet.to_string())
    tfile.close()
    #baselinecsvfilepath=bacthrun_results_path+testcase+"/"+filename+"baseline.csv"
    #baselineparquet.to_csv(baselinecsvfilepath)
    
  
    #print(outputparquet.equals(baselineparquet))
    if(not(outputparquet.equals(baselineparquet))):
        testcase_result_dict[filename]='Diff'
        isPassed='failure'
    else:
        testcase_result_dict[filename]='Same'
    cofile.write('\n')
    cbfile.write('\n')
  cofile.write('\n')
  cofile.write('\n')
  cbfile.write('\n')
  cbfile.write('\n')
  result_dict[testcase]=isPassed
  detailed_result_dict[testcase]=testcase_result_dict;
  executedcases = executedcases + 1
  printPercentage(executedcases,total_Jsoncases)
    
cofile.close()
cbfile.close()
#print(result_dict)
#print(detailed_result_dict)
generateHTMLReport(batchrunid,bacthrun_results_path,result_dict,filenames,detailed_result_dict,result_colors)






#subfolders = [ f.path for f in os.scandir(repo_path) if f.is_dir() ]
#print(subfolders)

#table1 = pq.read_table('/Users/1022177/BYRepository/dcro/resources/sampletestdata/OOPT-OGT-305.01/masterdata.parquet')
#table2 = pq.read_table('/Users/1022177/BYRepository/dcro/resources/sampletestdata/OOPT-OGT-305.03/masterdata.parquet')
#orderskucolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","supporderqty","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","sourcing"]

#outputfile="/Users/1022177/Desktop/dcroengineoutput/DCRO-100.01/ordersku.parquet"

#baselinefile="/Users/1022177/Downloads/output/DCRO-100.01/ordersku.parquet"

#columnnames=fileandcolumnnames[filename] 
#parquet1 = pq.read_table(outputfile,columns=orderskucolumns,memory_map=True)
#parquet2 = pq.read_table(baselinefile,columns=orderskucolumns,memory_map=True)
#print(parquet1.equals(parquet2))

#parquet1=pd.read_parquet(outputfile,columns=orderskucolumns)
#parquet2 = pd.read_parquet(baselinefile,columns=orderskucolumns)
#print(parquet1)
#parquet1.sort_values(by=['item'], inplace=True)
#parquet1.reset_index(drop=True, inplace=True)
#print(parquet1)

#tfile = open('test1.txt', 'w')
#tfile.write(parquet1.to_string())
#tfile.close()
#parquet1.to_csv('parquet1.csv') 
#parquet2.sort_values(by=['item'], inplace=True)
#parquet2.reset_index(drop=True, inplace=True)
#parquet2.to_csv('parquet2.csv') 
#tfile = open('test2.txt', 'w')
#tfile.write(parquet2.to_string())
#tfile.close()
#print(parquet2)
#print(parquet1.equals(parquet2))


#df = pd.read_parquet('/Users/1022177/BYRepository/dcro/resources/sampletestdata/OOPT-OGT-305.01/masterdata.parquet')
#df.to_csv('masterdata.csv')
