RUNNING TEST CASE SUITES AND SETUP FOR DCRO

Extract the stash in your local repository from [https://stash.jda.com/users/1022177/repos/dcrotestcases/browse]

After extracting, install python on your system. Mac already has a default python2.7.3 with it, but safer to install python >3.5.

Install XCode
The first step for Python 3 is to install Apple’s Xcode program which is necessary for iOS development as well as most programming tasks. We will use XCode to install Homebrew.
In your Terminal app, run the following command to install XCode and its command-line tools:
$ xcode-select --install
It is a large program so this make take a while to download. Make sure to click through all the confirmation prompts XCode requires.
Install Homebrew
Next install Homebrew by copy/pasting the following command into Terminal and then type Enter:
$ ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
To confirm Homebrew installed correctly, run this command:
$ brew doctor
Your system is ready to brew.
Install Python 3
Now we can install the latest version of Python 3. Type the following command into Terminal and press Enter:
$ brew install python3


After python is running, type python -version in terminal.

After this there are the following libraries required.

To ease installation, we will install pip in our terminal.

In your terminal run 
      sudo easy_install pip


After this install pyarrow, in your terminal type
	pip install pyarrow

After this install numpy in your terminal type
	Pip install numpy

After this install pandas, (with an ‘s’), in your terminal type
	Pip install pandas



open application.properties in your code. Set the following properties in that

isLocalFileStorage :true
outputstoragetype :parquet
inputdir :/Users/1022177/dcrotestcases/dcroengineinput/
outputdir :/Users/1022177/dcrotestcases/dcroengineoutput/

here inputdir will be path of dcroengineinput folder of dcrotestcases repository in your local machine.
In the dcrotestcases folder create a folder dcroengineoutput.
And set its path as outputdir in application.properties file.

Build the code 
a.	either through eclipse by Gradle -> Refresh Gradle Project
b.	or from terminal ./gradlew build


After this, in your local, bring the localhost:8080 hosting server up, or from Eclipse, using Run As Server mode.

After the server is up, execute the following python script.
	In your terminal type, 
Python executeTestCases.py
This will run the test cases.

There will be testCaseResult directory created, corresponding to the latest timestamp open the report and check 
	testcasereport.html

Or compare the two files using BeyondComapre or Compare it, 
	consolidatedBaseLineFile.txt and consolidatedoutputFile.txt.



Suggestions for further improvement:

1.	consolidate the all 8 files results
2.	handle the case where baselines are not present
3.	make a configuration parameter to run few testcases
6.      Move towards postgres(database) and text files as baselines
7. 	Asynhchronus request calls
8. 	Add PSR datasets once we freeze it(Performance)
9.	include Yourkit with this if possible.




