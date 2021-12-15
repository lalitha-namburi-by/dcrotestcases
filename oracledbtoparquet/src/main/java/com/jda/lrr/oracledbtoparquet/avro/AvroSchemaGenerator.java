package com.jda.lrr.oracledbtoparquet.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.jda.lrr.oracledbtoparquet.entity.ColumnDefination;
import com.jda.lrr.oracledbtoparquet.metadata.OracleDBMetaDataLoader;

/**
 * This class manages creation of avro schema for database table.
 * @author 1022177
 *
 */
public class AvroSchemaGenerator {

	/**
	 * This method creates avro schema for a table and all of it's columns.
	 *
	 * @param dbMetaDataLoader
	 * @param schemaName
	 * @param tableName
	 * @return {@link org.apache.avro.Schema}
	 */
	public Schema generateAvroSchemaForTable(OracleDBMetaDataLoader dbMetaDataLoader, String schemaName,
			String tableName) {
		return generateAvroSchemaForTableAndColumns(dbMetaDataLoader, schemaName, tableName, null);
	}

	/**
	 * This method creates avro schema for a table and it's specific columns.
	 * @param dbMetaDataLoader
	 * @param schemaName
	 * @param tableName
	 * @param columnNames
	 * @return {@link org.apache.avro.Schema}
	 */
	public Schema generateAvroSchemaForTableAndColumns(OracleDBMetaDataLoader dbMetaDataLoader, String schemaName,
			String tableName, List<String> columnNames) {
		return new Schema.Parser()
				.parse(generateSchemaStringForTable(dbMetaDataLoader, schemaName, tableName, columnNames));
	}

	/**
	 * This method creates the avro schema String for a table and it's columns.
	 * @param dbMetaDataLoader
	 * @param schemaName
	 * @param tableName
	 * @param columnNames
	 * @return {@link java.lang.String}
	 */
	public String generateSchemaStringForTable(OracleDBMetaDataLoader dbMetaDataLoader, String schemaName,
			String tableName, List<String> columnNames) {
		
        Map<String, ColumnDefination> columnsMap = dbMetaDataLoader.getColumnsDataTypesMap(schemaName, tableName);

		if (columnNames == null) {
			columnNames = new ArrayList<String>(columnsMap.keySet());
		}

		StringBuilder sb = new StringBuilder();
		sb.append("{").append("\n");
		sb.append("\t").append("\"type\"").append("\t").append(":").append("\t").append("\"record\",").append("\n");
		sb.append("\t").append("\"name\"").append("\t").append(":").append("\t").append("\"").append(tableName)
				.append("\",").append("\n");
		sb.append("\t").append("\"fields\"").append("\t").append(":").append("\t").append("[ ").append("\n");

		// iterating through all the required columns and converting them in
		// fields
		
		//TODO write the logic so that we don't need to make everything nullable and also correct default value should be provided
		//This will also require changes in ORacleTableDataDao and OracleMetaDataLoader class.
		for (int i = 0; i < columnNames.size(); i++) {

			ColumnDefination clmnDef = columnsMap.get(columnNames.get(i));
			//opening field 
			sb.append("\t").append("{").append("\n");
			
			//Appending the field Name to avro schema field
			sb.append("\t").append("\t").append("\"name\"").append("\t").append(":").append("\"")
					.append(columnNames.get(i).toLowerCase()).append("\",").append("\n");
			
			sb.append("\t").append("\t");
		
			//Appending the field Type to avro Schema field
			sb.append("\"type\" ").append(":");
			
			//creating Union type if field is nullable
			if (clmnDef.isNullable()) {
				sb.append(" [").append("\"null\"").append(", ");
			}
			
			if (clmnDef.getAvroLogicalType() != null) {
				sb.append(" {");
				sb.append("\"type\" ").append(":");
			}
			
			sb.append("\"").append(clmnDef.getAvroType()).append("\"");
			
			if (clmnDef.getAvroLogicalType() != null) {
				sb.append(",").append("\"logicalType\"").append(" : ").append("\"")
						.append(clmnDef.getAvroLogicalType()).append("\"");
			}
			
			if (clmnDef.getAvroLogicalType() != null) {
				sb.append(" }");
			}
			
			//closing Union type if field is nullable
			if (clmnDef.isNullable()) {
				sb.append("]");
			}
			
			//Appending default value to avro schema field if field is nullable
			if (clmnDef.isNullable()) {
				sb.append(",").append("\n");
				sb.append("\t").append("\t").append("\"default\"").append(" : ").append("null");
			}
			
			//closing the field
			sb.append("\n");
			sb.append("\t").append("}");

			if (i != columnNames.size() - 1) {
				sb.append(",").append("\n");
			}
		}

		sb.append("]").append("\n");
		sb.append("}");

		System.out.println(sb.toString());

		return sb.toString();
	}

}
