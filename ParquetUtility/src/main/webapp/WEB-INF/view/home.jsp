<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE HTML>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>BY Parquet Utility</title>
<style>
* {
  box-sizing: border-box;
}

body {
  font-family: Arial, Helvetica, sans-serif;
}

/* Style the header */
header {
  background-color: #666;
  padding: 30px;
  text-align: center;
  font-size: 35px;
  color: white;
}

/* Create two columns/boxes that floats next to each other */
nav {
  float: left;
  width: 30%;
  height: 300px; /* only for demonstration, should be removed */
  background: #ccc;
  padding: 20px;
}

/* Style the list inside the menu */
nav ul {
  list-style-type: none;
  padding: 0;
}

article {
  float: left;
  padding: 20px;
  width: 70%;
  background-color: #f1f1f1;
  height: 300px; /* only for demonstration, should be removed */
}

/* Clear floats after the columns */
section:after {
  content: "";
  display: table;
  clear: both;
}

/* Style the footer */
footer {
  background-color: #777;
  padding: 10px;
  text-align: center;
  color: white;
}
</style>

</head>

<body>

<header>
  <h2>BY Parquet Utility</h2>
</header>

<section>
  <nav>
    <ul>
      <li><a href="#">Fetch TestCase Data</a></li>
      <li><a href="#">Edit a Parquet File</a></li>
      <li><a href="/viewBasicTestCaseCreator">Create New TestCase</a></li>
      <li><a href="/getDBTestCasePage">Create TestCase From DB</a></li>
    </ul>
  </nav>
  
  <article>
    <div id ="testcasedata">
        <form action="/getTestCase">
       		<b>TestCase Name : </b> <input type = "text" name="testCaseName" id ="testcase"/>
       		<input type="hidden" name="isDirectFilePath" id="isDirectFilePath" value="false"/>
        	<INPUT type="submit" value="Get TestCase Data"  />
        </form>
    </div>
    <br/>
    <br/>
    <div>
        <b>File Path : </b>
        <br/>
		<textarea name="filepath" form="parquetform">Enter File Path here...</textarea>
    	<form action="/viewParquet" id="parquetform">
    		<input type="submit" value="View Parquet"/>
    	</form>
    </div>
  </article>
</section>

<footer>
  <p>Footer</p>
</footer>

</body>
</html>