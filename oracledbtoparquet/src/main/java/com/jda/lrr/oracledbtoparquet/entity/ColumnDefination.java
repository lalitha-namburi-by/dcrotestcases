package com.jda.lrr.oracledbtoparquet.entity;

import java.sql.Types;


import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;

public class ColumnDefination {

	private String columnName;
	private int sqlDataType;
	private boolean isNullable;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	public void setSqlDataType(int sqlDataType) {
		this.sqlDataType = sqlDataType;
	}

	public String getAvroType() {
		return toAvroType(this.sqlDataType).getName();
	}

	public String getAvroLogicalType() {
		switch (this.sqlDataType) {
		case Types.DATE:
			return LogicalTypes.date().getName();
		case Types.TIME:
			return LogicalTypes.timeMicros().getName();
		case Types.TIMESTAMP:
			return LogicalTypes.timestampMicros().getName();
		default:
			return null;
		}

	}

	/**
	 * 
	 * @param sqlType
	 * @return {@link org.apache.avro.Schema.Type}
	 */
	private Type toAvroType(int sqlType) {
		switch (sqlType) {

		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		
			return Type.INT;

		case Types.BIGINT:
		case Types.NUMERIC:
			return Type.LONG;

		case Types.BIT:
		case Types.BOOLEAN:
			return Type.BOOLEAN;

		case Types.REAL:
		case Types.DECIMAL:
			return Type.FLOAT;

		case Types.FLOAT:
		case Types.DOUBLE:
			return Type.DOUBLE;

		//case Types.NUMERIC:
		//case Types.DECIMAL:
		//	return Type.STRING;

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
			return Type.STRING;

		case Types.DATE:
			return Type.INT;
		case Types.TIME:
		case Types.TIMESTAMP:
			return Type.LONG;

		case Types.BLOB:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return Type.BYTES;

		default:
			throw new IllegalArgumentException("Cannot convert SQL type " + sqlType);
		}
	}

}
