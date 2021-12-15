package com.jda.lrr.oracledbtoparquet.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

import com.jda.lrr.oracledbtoparquet.entity.ParquetColDef;
import com.jda.lrr.oracledbtoparquet.entity.TableDefination;
import com.jda.lrr.oracledbtoparquet.jdbc.DataBaseConnectionFactory;

public class OracleTableDataInsert {

	public void insertDataToDB(Schema schema, TableDefination tableDef, List<ParquetColDef> colDefList,
			List<Record> tableData) {
		// TODO Auto-generated method stub

		int insertedRecords[];
		try (Connection conn = DataBaseConnectionFactory.getDBConnection()) {
			// Connection conn = DataBaseConnectionFactory.getDBConnection();
			conn.setAutoCommit(false);
			String query = buildSearchQuery(tableDef.getColumnNames(), tableDef.getTableName());
			System.out.println("Query=" + query);
			try (PreparedStatement pstmt = conn.prepareStatement(query)) {
				for (int i = 0; i < tableData.size(); i++) {
					int k = 1;
					for (ParquetColDef p : colDefList) {
						String colName = null;
						String sqlDataType = p.getSqlDataType();

						System.out.println("sqldatatype = " + sqlDataType);
						if (sqlDataType.equalsIgnoreCase("INTEGER")) {
							colName = p.getColumnName();
							pstmt.setInt(k, Integer.parseInt(String.valueOf(tableData.get(i).get(colName))));
						} else if (sqlDataType.equalsIgnoreCase("LONG")) {
							colName = p.getColumnName();
							pstmt.setLong(k, Long.parseLong(String.valueOf(tableData.get(i).get(colName))));
						} else if (sqlDataType.equalsIgnoreCase("DOUBLE")) {
							colName = p.getColumnName();
							pstmt.setDouble(k, Double.parseDouble(String.valueOf(tableData.get(i).get(colName))));
						} else if (sqlDataType.equalsIgnoreCase("BOOLEAN")) {
							colName = p.getColumnName();
							pstmt.setBoolean(k, Boolean.parseBoolean(String.valueOf(tableData.get(i).get(colName))));
						} else if (sqlDataType.equalsIgnoreCase("STRING")) {
							colName = p.getColumnName();
							pstmt.setString(k, String.valueOf(tableData.get(i).get(colName)));
						} else if (sqlDataType.equalsIgnoreCase("FLOAT")) {
							colName = p.getColumnName();
							pstmt.setFloat(k, Float.parseFloat(String.valueOf(tableData.get(i).get(colName))));
						} else if (sqlDataType.equalsIgnoreCase("DATE")) {
							colName = p.getColumnName();
							int timeSeconds = (Integer.parseInt(String.valueOf(tableData.get(i).get(colName)))) * 1000
									* 60 * 60 * 24;
							pstmt.setDate(k, (java.sql.Date) new Date(timeSeconds));
						} else if (sqlDataType.equalsIgnoreCase("TIMESTAMP")) {
							colName = p.getColumnName();
							int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now())
									.getTotalSeconds();
							long micro = Long.parseLong(String.valueOf(tableData.get(i).get(colName)));

							long timeStamp = (micro - (offset * 1000 * 1000)) / 1000;
							pstmt.setTimestamp(k, new Timestamp(timeStamp));
						}
						k++;
					}
					pstmt.addBatch();
					if ((i % 1000) == 0) {
						insertedRecords = pstmt.executeBatch();
					}

				}

				insertedRecords = pstmt.executeBatch();

				System.out.println("Succesfully inserted " + insertedRecords.length + " records");
				conn.commit();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn.rollback();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String buildSearchQuery(List<String> fields, String tableName) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT into").append(" ");
		sb.append(tableName).append(" ").append("values(");
		for (int i = 0; i < fields.size(); i++) {

			if (i == (fields.size() - 1)) {
				sb.append("?)");
			} else {
				sb.append("?,");
			}

		}

		return sb.toString();
	}

}
