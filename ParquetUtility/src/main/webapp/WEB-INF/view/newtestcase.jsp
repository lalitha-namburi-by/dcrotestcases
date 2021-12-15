<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<%@taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<html>
<head>
	<title>Basic Test Case Creator</title>
</head>
<body>
<a href="/"><input type="button" value ="GO TO HOME"/></a>
<h2>Create New Test Case</h2>
<form:form method="post" action="/createNewBasicTestCase" modelAttribute="newTestCaseForm">
	
	<input  type = "text" name="newTestCaseName" />
	
	
	<table>
	<tr>
		<th>suffix/prefix</th>
		<th>Value</th>
	</tr>
	<c:forEach items="${newTestCaseForm.suffixPrefixValueMap}" var="suffixPrefixEntry" varStatus="status">
		<tr>
			<td>${suffixPrefixEntry.key}</td>
			<td><input type="text" name="suffixPrefixValueMap['${suffixPrefixEntry.key}']" value="${suffixPrefixEntry.value}"/></td>
		</tr>
	</c:forEach>
	</table>
	
	<table>
	<tr>
		<th>column Name</th>
		<th>Default Value</th>
	</tr>
	<c:forEach items="${newTestCaseForm.defaultColumnValueMap}" var="defaultColumnValueEntry" varStatus="status">
		<tr>
			<td>${defaultColumnValueEntry.key}</td>
			<td><input type="text" name="defaultColumnValueMap['${defaultColumnValueEntry.key}']" value="${defaultColumnValueEntry.value}"/></td>
		</tr>
	</c:forEach>
	</table>	
<br/>

<input type="submit" value="Create New Test Case" />
	
</form:form>
</body>
</html>