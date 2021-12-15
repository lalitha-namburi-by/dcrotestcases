package com.jda.lrr.oracledbtoparquet.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.jda.lrr.oracledbtoparquet.entity.ColumnDefination;
import com.jda.lrr.oracledbtoparquet.jdbc.DataBaseConnectionFactory;


/**
 * This class manages the metadata of database.
 * @author 1022177
 *
 */
public class OracleDBMetaDataLoader {

	private static DatabaseMetaData dbMetaData = null;

	/**
	 * This method creates a map containing column names of table as keys 
	 * and their avroTypes as values.
	 * @param schemaName
	 * @param tableName
	 * @return {@link java.util.HashMap}
	 */
	public Map<String, ColumnDefination> getColumnsDataTypesMap(String schemaName, String tableName) {
		Map<String, ColumnDefination> columnsMap = new HashMap<String, ColumnDefination>();

		ResultSet columns = getColumnsMetaData(schemaName, tableName);

		try {
			while (columns.next()) {
				ColumnDefination clmnDef = new ColumnDefination();
				String columnName = columns.getString("COLUMN_NAME");
				clmnDef.setColumnName(columnName);
				clmnDef.setNullable("NO".equalsIgnoreCase(columns.getString("IS_NULLABLE")) ? false : true);
				clmnDef.setSqlDataType(columns.getInt("DATA_TYPE"));
				// int datatype = columns.getInt("DATA_TYPE");
				// String avroTypeString = toAvroType(datatype).getName();
				columnsMap.put(columnName, clmnDef);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return columnsMap;

	}

	/**
	 * This method fetches the metadata of columns of a table.
	 * @param schemaName
	 * @param tableName
	 * @return {@link java.sql.ResultSet}
	 */
	private ResultSet getColumnsMetaData(String schemaName,String tableName) {
		DatabaseMetaData dbMetaData = getDatabaseMetaData();
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
	 * @return
	 */
	private static DatabaseMetaData getDatabaseMetaData() {
		if (dbMetaData == null) {
			try {
				dbMetaData = DataBaseConnectionFactory.getDBConnection().getMetaData();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return dbMetaData;
	}

}
