function addRow(tableID) {

			var table = document.getElementById(tableID);
			var rowCount = table.rows.length;
			var row = table.insertRow(rowCount);
			var colCount = table.rows[1].cells.length;

			for(var i=0; i<colCount; i++) {
				var newcell	= row.insertCell(i);
				newcell.innerHTML = table.rows[1].cells[i].innerHTML;
				switch(newcell.childNodes[0].type) {
					case "text":
							newcell.childNodes[0].value = "";
							break;
					case "checkbox":
							newcell.childNodes[0].checked = false;
							break;
					case "select-one":
							newcell.childNodes[0].selectedIndex = 0;
							break;
				}
			}
		}

function copyRow(tableID) {

	var table = document.getElementById(tableID);
	var rowCount = table.rows.length;
	
	var rowToBeCopied; 
	for(var i=0; i<rowCount; i++) {
		var row = table.rows[i];
		var chkbox = row.cells[0].childNodes[0];
		if(null != chkbox && true == chkbox.checked) {
			rowToBeCopied = row;
			break;
		}

	 }
	
	
	var row = table.insertRow(rowCount);
	var colCount = table.rows[1].cells.length;

	
	
	for(var i=0; i<colCount; i++) {
		var newcell	= row.insertCell(i);
		newcell.innerHTML = table.rows[1].cells[i].innerHTML;
		
		var cellToBeCopied = rowToBeCopied.cells[i];
		switch(newcell.childNodes[0].type) {
			case "text":
					newcell.childNodes[0].value = cellToBeCopied.childNodes[0].value;
					break;
			case "checkbox":
					newcell.childNodes[0].checked = cellToBeCopied.childNodes[0].checked;
					break;
			case "select-one":
					newcell.childNodes[0].selectedIndex = cellToBeCopied.childNodes[0].selectedIndex;
					break;
		}
	}
}

		function deleteRow(tableID) {
			try {
			var table = document.getElementById(tableID);
			var rowCount = table.rows.length;

			for(var i=0; i<rowCount; i++) {
				var row = table.rows[i];
				var chkbox = row.cells[0].childNodes[0];
				if(null != chkbox && true == chkbox.checked) {
					if(rowCount <= 1) {
						alert("Cannot delete all the rows.");
						break;
					}
					table.deleteRow(i);
					rowCount--;
					i--;
				}

			 }
			}catch(e) {
				alert(e);
			}
		}
		
		function saveAll(gridnameslist) {
			  var list = gridnameslist.split("-");
			  for (index = 0; index < list.length; index++) { 
				    save(list[index]); 
				} 
		}

		function save(tableID){
			var testcase = document.getElementById("testcasename").innerText;
			var isdirectfilepath = document.getElementById("isDirectFilePath").value;
			var table = document.getElementById(tableID);
			var rowCount = table.rows.length;
			var colCount = table.rows[1].cells.length;
			var outputJSON= "{";
			outputJSON += ' "gridname" : "'+tableID+'" , ';
			outputJSON += '"griddata" : [';
			for(var i=2; i<rowCount; i++) {
				outputJSON += '[';
				for (var j = 1; j < colCount; j++) {
					var cell = table.rows[i].cells[j];
					outputJSON += '"';
					switch (cell.childNodes[0].type) {
							case "text":
								outputJSON += table.rows[i].cells[j].childNodes[0].value;
								break;
							case "checkbox":
								outputJSON += table.rows[i].cells[j].childNodes[0].checked;
								break;
							case "select-one":
								outputJSON += table.rows[i].cells[j].childNodes[0].selectedIndex;
								break;
					}
					 outputJSON += '"';
					if (j != (colCount - 1)) {
						outputJSON += " , ";
					}

				}
						
				outputJSON += ']';
				if (i != (rowCount - 1)) {
					outputJSON += " , ";
				}
		    }
				
			outputJSON += ']';
			outputJSON += ' }';

			if(isdirectfilepath == "true"){
				var encodedfilePath = encodeURI(testcase);
				fire_ajax_submit("/updateParquet?filePath="+encodedfilePath,outputJSON);
			}
			else{
				fire_ajax_submit("/save/"+testcase,outputJSON);
			}
			console.log(outputJSON);
		    //alert(outputJSON);
	     }
		
		function fire_ajax_submit( saveurl, jsondata) {

		     $.ajax({
		        type: "POST",
		        contentType: "application/json",
		        url: saveurl,
		        data: jsondata,
		        dataType: 'json',
		        cache: false,
		        timeout: 600000,
		        success: function (data) {
		        	
		        },
		        error: function (e) {
		        	
		        }
		    });

		}
		
		