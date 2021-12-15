@ECHO OFF

ECHO %CD%
Echo Launch dir: "%~dp0"
Echo Current dir: "%CD%"

java -jar %~dp0\oracledbtoparquetutility-jar-with-dependencies.jar %~dp0\application.properties
PAUSE