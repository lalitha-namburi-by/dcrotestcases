package com.blueyonder.dcro.parquettestcasedeveloper.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class DBMetaDataLoader {
	
	private static DatabaseMetaData dbMetaData = null;

	/**
	 * This method creates a map containing column names of table as keys and
	 * their avroTypes as values.
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return {@link java.util.HashMap}
	 */
	public Map<String, Integer> getColumnsDataTypesMap(String schemaName, String tableName,Connection connection) {
		Map<String, Integer> columnsMap = new HashMap<String, Integer>();

		ResultSet columns = getColumnsMetaData(schemaName, tableName,connection);

		try {
			while (columns.next()) {

				String columnName = columns.getString("COLUMN_NAME");
				int datatype = columns.getInt("DATA_TYPE");
				columnsMap.put(columnName, datatype);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return columnsMap;

	}

	/**
	 * This method fetches the metadata of columns of a table.
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return {@link java.sql.ResultSet}
	 */
	private ResultSet getColumnsMetaData(String schemaName, String tableName,Connection connection) {
		DatabaseMetaData dbMetaData = getDatabaseMetaData(connection);
		ResultSet columns = null;

		try {
			columns = dbMetaData.getColumns(null, schemaName, tableName, null);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return columns;
	}

	/**
	 * This method fetches the metadata of database from connection.
	 * 
	 * @return
	 */
	private  DatabaseMetaData getDatabaseMetaData(Connection connection) {
		if (dbMetaData == null) {
			try {
				dbMetaData = connection.getMetaData();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return dbMetaData;
	}
	
	public void emptyDBMetaData() {
		dbMetaData = null;
	}

}
