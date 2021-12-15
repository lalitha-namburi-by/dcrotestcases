package com.jda.lrr.oracledbtoparquet.entity;

import java.util.ArrayList;
import java.util.List;

import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;

/**
 * 
 * @author 1022177
 * This class builds a list of tables definitions objects by reading the tables names and their column names
 * from properties file to load from oracle Db and write to parquet file.
 * 
 */
public class TablesDefinationsListBuilder {
	
	/**
	 * 
	 * This method loads the table names from properties file and creates a list of table definition objects
	 * which contains table name and it's columns which needs to be fetched from oracle db.
	 * @return {@link java.util.ArrayList}
	 */
	public List<TableDefination> getTablesDefinationsList() {
		List<TableDefination> tablesDefinationsList = new ArrayList<TableDefination>();
		List<String> tableNames = PropertyFileLoader.getPropertiesValueAsList("table.names");
		for (String tableName : tableNames) {
			TableDefination tableDefObj = new TableDefination();
			tableDefObj.setTableName(tableName);
			tableDefObj.setColumnNames(buildColumnList(tableName));
			tablesDefinationsList.add(tableDefObj);
		}
		return tablesDefinationsList;
	}

	/**
	 * This method loads the column names of a table from properties file.
	 * @param tableName
	 * @return {@link java.util.ArrayList}
	 */
	private List<String> buildColumnList(String tableName) {
		return PropertyFileLoader.getPropertiesValueAsList("table." + tableName.toLowerCase() + ".columns");
	}

}
