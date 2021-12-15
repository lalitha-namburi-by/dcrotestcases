<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js"></script>
<script src="/js/main.js"></script>
<title>Parquet Utility</title>
<SCRIPT>
</SCRIPT>
</head>
<body>
    <div>
    
        <div>
           <a href="/"><input type="button" value ="GO TO HOME"/></a>
           <form action="/getTestCase">
       		<input type = "hidden" name="testCaseName" value ="${testcasename}" id ="testcase"/>
       		<input type="hidden" name="isDirectFilePath" id="isDirectFilePath" value="${isdirectfilepath}"/>
        	<INPUT type="submit" value="Refresh"  />
           </form>
           
           
           <INPUT type="button" value="Save All" onclick="saveAll('${gridnameslist}')" />
           
           <br/>
           <form action="/viewTestCaseCreatorFromAnotherTestCase" method="post">
       		<input type = "hidden" name="oldTestCaseName" value ="${testcasename}" id ="testcase"/>
       		<INPUT type="submit" value="Create New Test Case From This"  />
           </form>
           
           <div>Test Case Name : <span id="testcasename">${testcasename}</span></div>
           
           
           
           <c:forEach var="grid" items="${gridlist}">
            <H1>${grid.getGridName()}</H1> 
            <INPUT type="button" value="Add Row" onclick="addRow('${grid.getGridName()}')" />
            <INPUT type="button" value="copy Row" onclick="copyRow('${grid.getGridName()}')" />
            <INPUT type="button" value="Delete Row" onclick="deleteRow('${grid.getGridName()}')" />
			<INPUT type="button" value="save" onclick="save('${grid.getGridName()}')" />
			
            <TABLE id="${grid.getGridName()}"  border="1">
            	<TR>
            		<TH>
            	
            		</TH>
            		
            		<c:forEach var="columnData" items="${grid.getColumnsDetail()}">
            		<TH> ${columnData.getColumnName()} <br/> ( ${columnData.getColumnDataType()} ) </TH>
            		</c:forEach>
            	
            	</TR>
			
				<TR style="display:none;">
            		<TD><INPUT type="checkbox" name="chk"/></TD>
            		
            		<c:forEach var="columnData" items="${grid.getColumnsDetail()}">
            		<TD><INPUT type="text" name="txt" value=""/></TD>
            		</c:forEach>
            	
            	</TR>
			 <c:forEach var="rowData" items="${grid.getGridData()}">
			 	<TR>
			 			<TD><INPUT type="checkbox" name="chk"/></TD>
            		<c:forEach var="elem" items="${rowData}" varStatus="rowIndex">
            		
            			<!--<c:forEach var="columnData" items="${grid.getColumnsDetail()}" varStatus="columnIndex">
            				<c:if test="${rowIndex.index == columnIndex.index}" >
          				 		<c:set var="type" value="${columnData.getColumnDataType()}" scope="page" />
      			 			</c:if>
      			 		</c:forEach>
            			
            			<c:choose>
        					<c:when test="${type == 'string' }">
          						<TD><INPUT type="text" name="txt" value="${elem}"/></TD>
          
        					</c:when>
        					<c:when test="${type == 'double' }">
          						<TD><INPUT type="text" name="txt" value="${elem}"/></TD>
          
        					</c:when>
        					<c:when test="${type == 'boolean' }">
          						<TD><INPUT type="checkbox" name="txt" value="${elem}"/></TD>
          
        					</c:when>
        					<c:when test="${type == 'date' }">
          						<TD><INPUT type="datetime-local" name="txt" value="${elem}"/></TD>
          
        					</c:when>
       					   
     					</c:choose>-->
            			
            			<TD><INPUT type="text" name="txt" value="${elem}"/></TD>
            		</c:forEach>
            	</TR>
            </c:forEach>
            
			</TABLE>
		 </c:forEach>
        </div>
    </div>
    
    
</body>
</html>
