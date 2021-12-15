package com.blueyonder.dcro.parquettestcasedeveloper.repository;

import java.util.Date;

import org.springframework.stereotype.Component;

import com.blueyonder.dcro.parquettestcasedeveloper.entities.GridColumn;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.ParquetGrid;

@Component
public class ParquetDataRepository {

	public ParquetGrid buildGridParquet(String gridName){
		GridColumn[] gridColumns = getColumnDetails();
		ParquetGrid pd  = new ParquetGrid(gridName,getNoOfColumns(gridColumns));
		pd.addColumnDetails(gridColumns);
		
		Object[] obj1 = new Object[5];
		obj1[0] = "id1";
		obj1[1] = "name1";
		obj1[2] = 4000.0;
		obj1[3] = new Date();
		obj1[4] = true;
		pd.addRowData(obj1);
		
		Object[] obj2 = new Object[5];
		obj2[0] = "id2";
		obj2[1] = "name2";
		obj2[2] = 3000.0;
		obj2[3] = new Date();
		obj2[4] = true;
		pd.addRowData(obj2);
		
		Object[] obj3 = new Object[5];
		obj3[0] = "id3";
		obj3[1] = "name3";
		obj3[2] = 6000.0;
		obj3[3] = new Date();
		obj3[4] = false;
		pd.addRowData(obj3);
		
		Object[] obj4 = new Object[5];
		obj4[0] = "id4";
		obj4[1] = "name4";
		obj4[2] = 5000.0;
		obj4[3] = new Date();
		obj4[4] = true;
		pd.addRowData(obj4);
		return pd;
	}
	
	public int getNoOfColumns(GridColumn[] columnArray){
		return columnArray.length;
	}
	
	public GridColumn[] getColumnDetails(){
		
		GridColumn[] gridColumns = new GridColumn[5];
		GridColumn gc1 = new GridColumn();
		gc1.setColumnName("employeeId");
		gc1.setColumnDataType("string");
		gc1.setNullable(false);
		//gc1.setDefaultValue(defaultValue);
		gridColumns[0] = gc1;
		
		GridColumn gc2 = new GridColumn();
		gc2.setColumnName("firstName");
		gc2.setColumnDataType("string");
		gc2.setNullable(false);
		gridColumns[1] = gc2;
		
		GridColumn gc3 = new GridColumn();
		gc3.setColumnName("salary");
		gc3.setColumnDataType("double");
		gc3.setNullable(false); 
		gridColumns[2] = gc3;
		
		GridColumn gc4 = new GridColumn();
		gc4.setColumnName("joiningdate");
		gc4.setColumnDataType("date");
		gc4.setNullable(false);
		gridColumns[3] = gc4;
		
		GridColumn gc5 = new GridColumn();
		gc5.setColumnName("isMarried");
		gc5.setColumnDataType("boolean");
		gc5.setNullable(false);
		gridColumns[4] = gc5;
		
		return gridColumns;
	}
}
