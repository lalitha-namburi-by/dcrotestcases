#!/usr/bin/env python3
import os

current_dir = os.getcwd()
batch_cases_file_path = current_dir + "/summaryCases"

testCasesStringList = []
with open(batch_cases_file_path) as file :
	for testCaseLine in file :
		testCaseLine = testCaseLine.strip()
		testCasesStringList.append(testCaseLine)

for testCaseString in testCasesStringList:
    testCaseData = testCaseString.split(":")
    testCaseName = testCaseData[0]
    orderPlaceDate = testCaseData[1]
    script_command = "./createSummary.py"+" "+testCaseName+" "+orderPlaceDate
    os.system(script_command)