package com.jda.lrr.oracledbtoparquet.entity;

import java.sql.Types;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class ParquetColDef {

	private String columnName;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result + ((sqlDataType == null) ? 0 : sqlDataType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ParquetColDef other = (ParquetColDef) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (isNullable != other.isNullable)
			return false;
		if (sqlDataType != other.sqlDataType)
			return false;
		return true;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getSqlDataType() {
		return toOracleDBDatatypes(this.sqlDataType);
	}

	public void setSqlDataType(String sqlDataType) {
		this.sqlDataType = sqlDataType;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public void setNullable(boolean isNullable2) {
		this.isNullable = isNullable2;
	}

	public ParquetColDef() {

	}

	public ParquetColDef(String columnName, String sqlDataType, boolean isNullable) {
		super();
		this.columnName = columnName;
		this.sqlDataType = sqlDataType;
		this.isNullable = isNullable;
	}

	private String sqlDataType;
	private boolean isNullable;

	public String getOracleLogicalType(String logTypes) {
		switch (logTypes) {
		case "date":
			return "DATE";
		case "time-micros":
			return "TIME";
		case "timestamp-micros":
			return "TIMESTAMP";
		default:
			return null;
		}

	}

	public String toOracleDBDatatypes(String sqlDataType2) {
		switch (sqlDataType2) {

		case "int":
			return "INTEGER";

		case "long":
			return "LONG";

		case "boolean":
			return "BOOLEAN";

		case "float":
			return "FLOAT";

		case "double":
			return "DOUBLE";

		case "string":
			return "STRING";

		case "enum":
			return "STRING";

		case "bytes":
			return "LONGVARBINARY";
		case "TIMESTAMP":
			return "TIMESTAMP";
		case "TIME":
			return "TIME";
		case "DATE":
			return "DATE";

		default:
			throw new IllegalArgumentException("Cannot convert SQL type " + sqlDataType2);

		}
	}
}
