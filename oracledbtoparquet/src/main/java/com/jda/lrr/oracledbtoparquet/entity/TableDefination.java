package com.jda.lrr.oracledbtoparquet.entity;

import java.util.List;

/**
 * This class stores the table name and list of it's columns. 
 * @author 1022177
 *
 */
public class TableDefination {

	private String tableName;
	private List<String> columnNames;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

}
