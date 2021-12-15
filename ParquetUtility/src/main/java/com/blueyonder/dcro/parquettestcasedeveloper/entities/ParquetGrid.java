package com.blueyonder.dcro.parquettestcasedeveloper.entities;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ParquetGrid {

	@JsonProperty("gridname")
	private String gridName;

	private GridColumn[] columnsDetail;

	public GridColumn[] getColumnsDetail() {
		return columnsDetail;
	}

	private int noofcolumns;
	
	@JsonProperty("griddata")
	private List<Object[]> gridData;

	public ParquetGrid(String gridName, int noofcolumns) {

		this.gridName = gridName;
		this.noofcolumns = noofcolumns;
		this.columnsDetail = new GridColumn[noofcolumns];
		this.gridData = new ArrayList<Object[]>();

	}
	
	
	public void addColumnDetail(int index,GridColumn gridColumn){
		if(index >= noofcolumns)
			return;
		this.columnsDetail[index] = gridColumn;
		
	}
	
	public void addColumnDetails(GridColumn[] gridColumns){
		this.columnsDetail = gridColumns;
	}
	
	public void addRowData(Object[] rowData){
		if(rowData.length != noofcolumns)
			return;
		this.gridData.add(rowData);
	}

	public String getGridName(){
		return gridName;
	}
	
	public List<Object[]> getGridData(){
		return this.gridData;
	}
}
