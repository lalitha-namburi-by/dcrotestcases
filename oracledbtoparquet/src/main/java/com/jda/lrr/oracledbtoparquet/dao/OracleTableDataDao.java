package com.jda.lrr.oracledbtoparquet.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;

import com.jda.lrr.oracledbtoparquet.jdbc.DataBaseConnectionFactory;
import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;

/**
 * This class fetches the data of a table and writes Generic Data records to parquet
 * @author 1022177
 *
 */
public class OracleTableDataDao {
	
	/**
	 * This method builds a query for an avro schema then fetches the data 
	 * from database. 
	 * @param schema
	 * @return {@link java.util.ArrayList}
	 */
	public void writeTableDataToParquet(ParquetWriter<GenericData.Record> writer,Schema schema,String condition) {
		ResultSet rs = null;
		Statement stmt = null;
		String searchQuery = buildSearchQuery(schema.getFields(), schema.getName(),condition);
		Connection conn = DataBaseConnectionFactory.getDBConnection();
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(searchQuery);
			writeResultSetDataToParquet(writer,schema, rs);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method builds a generic data records from ResultSet and Schema.
	 * And writes the record to parquet file.
	 * @param schema
	 * @param rs
	 * @return {@link java.util.ArrayList}
	 */
	private void writeResultSetDataToParquet(ParquetWriter<GenericData.Record> writer, Schema schema, ResultSet rs) {
		
		List<Field> fields = schema.getFields();
		try {
			while (rs.next()) {
				GenericData.Record record = new GenericData.Record(schema);
				for (Field field : fields) {

					Type fieldType = field.schema().getType();
					LogicalType logicalType = field.schema().getLogicalType();
					if (fieldType == Schema.Type.UNION) {
						fieldType = field.schema().getTypes().get(1).getType();
						logicalType = field.schema().getTypes().get(1).getLogicalType();

					}

					if (fieldType == Schema.Type.INT) {
						if (logicalType == null) {
							record.put(field.name(), rs.getInt(field.name()));
						} else {
							record.put(field.name(),
									(int) (rs.getDate(field.name()).getTime() / (1000 * 60 * 60 * 24)));
						}
					} else if (fieldType == Schema.Type.STRING) {
						record.put(field.name(), rs.getString(field.name()));
					} else if (fieldType == Schema.Type.BOOLEAN) {
						record.put(field.name(), rs.getBoolean(field.name()));
					} else if (fieldType == Schema.Type.DOUBLE) {
						record.put(field.name(), rs.getDouble(field.name()));
					} else if (fieldType == Schema.Type.FLOAT) {
						record.put(field.name(), rs.getFloat(field.name()));
					} else if (fieldType == Schema.Type.LONG) {
						if (logicalType == null) {
							record.put(field.name(), rs.getLong(field.name()));
						} else if (logicalType.getName().equalsIgnoreCase(LogicalTypes.timestampMicros().getName())) {

							int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now())
									.getTotalSeconds();
							if(rs.getTimestamp(field.name())!=null){
								long micros = (rs.getTimestamp(field.name()).getTime() + (offset * 1000)) * 1000;
								record.put(field.name(), micros);
							}
							
						}else if(logicalType.getName().equalsIgnoreCase(LogicalTypes.timestampMillis().getName())){
							int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now())
									.getTotalSeconds();
							if (rs.getTimestamp(field.name()) != null) {
								long millis = rs.getTimestamp(field.name()).getTime() + (offset * 1000);
								record.put(field.name(), millis);
							}
						}
					} else if (fieldType == Type.BYTES) {
						record.put(field.name(), rs.getBytes(field.name()));
					}

				}
				writer.write(record);
			}
            
			writer.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * This method builds a query to fetch the data from a table.
	 * @param fields
	 * @param tableName
	 * @return {@link java.util.String}
	 */
	private String buildSearchQuery(List<Field> fields, String tableName,String condition) {
		
		String dbQuery = PropertyFileLoader.getPropertiesValueAsString(tableName.toLowerCase()+".table.query");
		if(dbQuery !=null){
			return dbQuery;
		}
		
		if(fields.size() == 0){
			//TODO Need to throw an exception  from here because table does not have any columns.
		}
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT").append(" ");
		for (int i = 0; i < fields.size(); i++) {
			sb.append(fields.get(i).name());
			if (i < (fields.size() - 1)) {
				sb.append(" , ");
			}
		}
		sb.append(" FROM ");
		sb.append(tableName);
		
		if(condition != null && condition.length()>0){
			sb.append(" ");
			sb.append(condition);
		}
		
		return sb.toString();
	}

}
