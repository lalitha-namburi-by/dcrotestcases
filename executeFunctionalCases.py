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

    message += "</tr>"
    for test_id in result_dict:
        # create the colored cell:
        color = result_colors[result_dict[test_id]]
        message += "<tr>"
        message += "<td>"+test_id+"</td>"
        message += "<td bgcolor='"+color+"'>"+result_dict[test_id]+"</td>"
        if(result_dict[test_id] == 'success'):
            passed=passed+1

        defaultMessage = 'Skipped due to catastrophic error.'
        detailed_result = detailed_result_dict.get(test_id, defaultMessage)
        if (isinstance(detailed_result, str)):
            message += "<td>"+defaultMessage+"</td>"
        else:
            for filename in filenames:
                message += "<td>"+detailed_result[filename]+"</td>"

        path = bacthrun_results_path+"/"+test_id+"/"
        message += "<td>"+path+"</td>"

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
    batchrunid="Report-"+str(current_time)


print(batchrunid)

#input_data_path="/Users/1022177/Desktop/PythonScripts/dcroengineinput/"
current_dir = os.getcwd()

output_data_path=current_dir+"/dcroengineoutput/BATCH/"

baseline_data_path=current_dir+"/outputbaselines/BATCH/"

results_path=current_dir+"/testresults/"

bacthrun_results_path=results_path+batchrunid+"/"

result_colors = {
        'success':      'lime',
        'failure':      'red',
        'error':        'yellow',
    }

#testcases=["DCRO-100.01", "DCRO-100.02", "DCRO-100.03","DCRO-100.04", "DCRO-100.05", "DCRO-100.06","DCRO-100.07", "DCRO-100.08", "DCRO-100.09","DCRO-100.10", "DCRO-100.11", "DCRO-100.12","DCRO-100.13", "DCRO-100.14", "DCRO-100.15","DCRO-100.16", "DCRO-100.17", "DCRO-100.18","DCRO-100.19", "DCRO-100.20", "DCRO-100.21","OOPT-GAA319.03", "OOPT-GAA319.07", "OOPT-GAA319.09","OOPT-GAA319.11", "OOPT-GAA319.13", "OOPT-GAA319.15", "OOPT-GAA319.17","OOPT-GEX334.01","OOPT-GEX334.02","OOPT-GEX334.03","OOPT-GEX334.04","OOPT-GEX334.05","OOPT-GEX334.09","OOPT-GEX334.10"]

testcases=["DCRO-100.01","DCRO-100.02","DCRO-100.03","DCRO-100.04","DCRO-100.05","DCRO-100.06","DCRO-100.07","DCRO-100.08","DCRO-100.09","DCRO-100.10","DCRO-100.11","DCRO-100.12","DCRO-100.13","DCRO-100.14","DCRO-100.15","DCRO-100.16","DCRO-100.17","DCRO-100.18","DCRO-100.19","DCRO-100.20","DCRO-100.21","DCRO-100.22","DCRO-100.23","DCRO-100.24","DCRO-100.25","DCRO-100.26","DCRO-100.27","DCRO-100.28","DCRO-100.29","DCRO-100.30","DCRO-100.31","DCRO-100.32","DCRO-100.33","DCRO-100.34","DCRO-100.35","DCRO-100.36","DCRO-100.37","OOPT-GAA319.01","OOPT-GAA319.03","OOPT-GAA319.05","OOPT-GAA319.06","OOPT-GAA319.07","OOPT-GAA319.09","OOPT-GAA319.11","OOPT-GAA319.13","OOPT-GAA319.14","OOPT-GAA319.15","OOPT-GAA319.17","OOPT-GAA319.18","OOPT-GAA319.22","OOPT-GEX334.01","OOPT-GEX334.02","OOPT-GEX334.03","OOPT-GEX334.04","OOPT-GEX334.05","OOPT-GEX334.06","OOPT-GEX334.07","OOPT-GEX334.08","OOPT-GEX334.09","OOPT-GEX334.10","OOPT-GEX334.11","OOPT-GEX334.12","OOPT-GEX334.13","OOPT-GEX334.14","OOPT-GOA504.01","OOPT-GOA504.03","OOPT-GOA504.05","OOPT-GOA504.07","OOPT-GOA504.08","OOPT-GOA504.09","OOPT-GOA504.10","OOPT-GOA504.11","OOPT-GOA504.12","OOPT-GOA504.14","OOPT-GOA504.15","OOPT-GOA504.16","OOPT-GOA504.19","OOPT-GOA504.21","OOPT-GOA504.23","OOPT-GOA504.25","OOPT-GOA504.27","OOPT-GOR502.01","OOPT-GOR502.02","OOPT-GOR502.03","OOPT-GOR502.04","OOPT-GOR502.05","OOPT-GOR502.06","OOPT-GOR502.07","OOPT-GOR502.08","OOPT-GOR502.09","OOPT-GOR502.10","OOPT-GOR502.11","OOPT-GOR502.12","OOPT-GOR502.13","OOPT-GOR502.14","OOPT-GOR502.15","OOPT-GOR502.16","OOPT-GOR502.17","OOPT-GOR502.18","OOPT-GOR502.19","OOPT-GOR502.20","OOPT-GOR502.21","OOPT-GOR502.22","OOPT-GOR502.23","OOPT-GOR502.25","OOPT-GOR502.27","OOPT-GSD335.01","OOPT-GSD335.02","OOPT-GSD335.04","OOPT-GSD335.06","OOPT-GSD335.07","OOPT-GSD335.08","OOPT-GSD335.10","OOPT-GSD335.15","OOPT-GSD335.16","OOPT-GSD335.17","OOPT-GSD335.18","OOPT-IGN13.01","OOPT-IGN13.02","OOPT-IGN13.03","OOPT-IGN13.04","OOPT-MBG327.01","OOPT-MBG327.02","OOPT-MBG327.03","OOPT-MBG327.04","OOPT-MBG327.05","OOPT-MBG327.06","OOPT-MBG327.07","OOPT-MBG327.08","OOPT-MBG327.09","OOPT-MBG327.10","OOPT-MBG327.11","OOPT-MBG327.12","OOPT-MBG327.13","OOPT-MBG327.14","OOPT-MBG327.15","OOPT-MBG327.16","OOPT-MBG327.17","OOPT-MBG327.18","OOPT-MBG327.19","OOPT-MBG327.20","OOPT-MBG327.21","OOPT_AAA12.01","OOPT_AAA12.02","OOPT_AAA12.03","OOPT_AAA12.04","OOPT_AAA12.05","OOPT_NDB11.01","OOPT_NDB11.02","OOPT_NDB11.03","OOPT-FBK414.01","OOPT-FBK414.02","OOPT-FBK414.03","OOPT-FBK414.04","OOPT-FBK414.05","OOPT-FBK414.06","OOPT-FBK414.07","OOPT-FBK414.08","OOPT-FBK414.09","OOPT-FBK414.10","OOPT-FBK414.11","OOPT-FBK414.12","OOPT-FBK414.13","OOPT-FBK414.14","OOPT-FBK414.15","OOPT-FBK414.16","OOPT-FBK414.17","OOPT-FBK414.18","OOPT-FBK414.19","OOPT-FBK414.20","OOPT-FBK414.21","OOPT-FBK414.22"]

orderheadercolumns=["source","dest","transmode","createdate","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype"]
orderheadersortcolumns=["source","dest","transmode","createdate","orderplacedate","departuredate","deliverydate","arrivdate","totalleadtime","transitdur","unloaddur","needcovdur","mincovdur","finalcovdate","finalcovdur","delaydur","orderskucount","orderskusoqcount","networkmincovdur","networkmincovdate","needcovdate","maxcovdur","orderbuildrule","duestatus","networkminstatus","loadsolutionstatus","loadstatus","approvalstatus","precisionbuildsw","ordertype","ordergroup","ordergroupmember","ordergroupparam","ordergroupbuildrule","precisionloadsw","vehicleloadcount","lanetype"]

orderexceptioncolumns = ["exception","exceptiondate","descr","item","source","dest","transmode","ordergroup","ordergroupmember"]
#orderexceptionsortcolumns=["source","dest","transmode","item","exception"]
orderexceptionsortcolumns=["exception","exceptiondate","descr","item","source","dest","transmode","ordergroup","ordergroupmember"]

orderskucolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","supporderqty","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","sourcing"]
orderskusortcolumns=["item","dest","source","transmode","arrivdate","ordercovdate","mincovdate","orderpointdate","orderpointprojoh","orderpointssqty","status","adjskucovdate","orderuptoleveldate","orderuptolevelssqty","orderuptolevelprojoh","unroundedsoq","soq","delaydur","soqcovdur","systemsoq","expdate","restrictedsoqcovdate","soqrestriction","unrestrictedsoq","orderpointavailsupply","orderplacedateprojoh","orderuptolevelavailsupply","orderpointadjreasons","orderuptoleveladjreasons","supporderqty","calcsoqsw","finalunitcost","ordergroup","ordergroupmember","orderplacessdisplayqty","orderpointssdisplayqty","orderuptolevelssdisplayqty","sourceinvstatus","unconstrsoq","ohpost","oh","sourcing"]

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

consolidatedoutputfile = bacthrun_results_path+"consolidatedoutputfile.txt"
consolidatedbaselinefile = bacthrun_results_path+"consolidatedbaselinefile.txt"
createDirectory(bacthrun_results_path)

cofile = open(consolidatedoutputfile, 'w')
cbfile = open(consolidatedbaselinefile, 'w')

print("executing testcases now !!")

argsdata = sys.argv
argslen = len(argsdata)

if argslen > 1:
 #testcases.clear()
 commandlinedata = argsdata[1]
 testcases = commandlinedata.split(",")

total_cases = len(testcases)

executedcases = 0
print("\n")
for testcase in testcases:
  #print(testcase)
  testcase = testcase.strip()
  if(testcase == ""):
    continue;

  url = 'http://localhost:8080/dcro_engine_service/trigger'
  inputdata = {'inputFolderName':testcase, 'orderPlaceDate':'2006-10-30'}
  res = requests.post(url, json =inputdata)
  #print(x.text)
  
  isPassed='success';
  
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
  printPercentage(executedcases,total_cases)

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
