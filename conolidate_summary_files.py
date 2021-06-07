import glob

import os

current_dir = os.getcwd()
batch_cases_file_path = current_dir + "/summaryCases"

testCasesStringList = []
with open(batch_cases_file_path) as file :
    for testCaseLine in file :
        testCaseLine = testCaseLine.strip()
        testCasesStringList.append(testCaseLine)

read_files = []
for testCaseString in testCasesStringList:
    testCaseData = testCaseString.split(":")
    testCaseName = testCaseData[0]
    filepath = current_dir + "/dcroengineoutput/" + testCaseName +"/summary.txt"
    read_files.append(filepath)


#read_files = glob.glob("/dcroengineoutput/*/summary.txt")
print(read_files)

with open('result.txt', 'w') as outfile:
    for fname in read_files:
        with open(fname) as infile:
            outfile.write('\n')
            outfile.write('\n')
            outfile.write(fname)
            outfile.write('\n')
            for line in infile:
                outfile.write(line)