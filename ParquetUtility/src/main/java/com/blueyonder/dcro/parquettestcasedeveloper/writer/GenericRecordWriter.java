package com.blueyonder.dcro.parquettestcasedeveloper.writer;


import com.blueyonder.dcro.parquettestcasedeveloper.database.DBMetaDataLoader;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.DBTestCaseEntity;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.GridColumn;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.MyParquetOutputFile;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.ParquetGrid;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component
public class GenericRecordWriter {
  
  private static final String STRING_TYPE = "string";
  private static final String DATE_TYPE = "date";
  private static final String BOOLEAN_TYPE = "boolean";
  private static final String INTEGER_TYPE = "integer";
  private static final String DAYS_TYPE = "days";
  private static final String RULE_TYPE = "rule";
  private static final String TIMESTAMP_TYPE = "timestamp";
  private static final String DOUBLE_TYPE = "double";
  private static final String LONG_TYPE = "long";
  private static final long LATE_DATE = 1073727360L;

  private static final long ORDER_DATE_FROM = 0L;
  private static final long ORDER_DATE_UPTO = 0L;
  private static final long FALLBACK_DAYS = 4;

  public void writeGridToParquetFile(String filePath, Schema schema, ParquetGrid grid) {
    ParquetWriter<GenericData.Record> writer = buildParquetWriter(filePath, schema);

    GridColumn[] columnDetails = grid.getColumnsDetail();
    int colCount = grid.getColumnsDetail().length;
    for (Object[] rowData : grid.getGridData()) {
      GenericData.Record record = new GenericData.Record(schema);

      for (int i = 0; i < colCount; i++) {
        GridColumn gridColumn = columnDetails[i];
        String key = gridColumn.getColumnName();
        Object value = rowData[i];
        if (value == null) {
          record.put(key, null);
          continue;
        }
        switch (gridColumn.getColumnDataType()) {
          case "string":
            if (value instanceof String)
              record.put(key, (String) value);
            break;
          case "double":
          case "days":
            if (value instanceof String) {
              System.out.println(key + " : " + value);
              if (value != null && !((String) value).isEmpty()) {
                record.put(key, Double.valueOf((String) value));
              } else {
                if (gridColumn.isNullable()) {
                  record.put(key, null);
                } else {
                  record.put(key, 0.0);
                }
              }
            } else {
              record.put(key, value);
            }
            break;
          case "boolean":
            if (value instanceof String) {
              record.put(key, Boolean.valueOf((String) value));
            } else {
              record.put(key, value);
            }
            break;
          case "long":
          case "timestamp":
          case "date":
          case "rule":
            if (value instanceof String) {
              record.put(key, convertStringToLong((String) value));
            } else {
              record.put(key, value);
            }
            break;
          case "integer":
            if (value instanceof String) {
              if (value != null && !((String) value).isEmpty()) {
                record.put(key, Integer.valueOf((String) value));
              } else {
                record.put(key, null);
              }

            } else {
              record.put(key, value);
            }
            break;
        }

      }
      try {
        writer.write(record);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  private ParquetWriter<GenericData.Record> buildParquetWriter(String fileToWrite, Schema schema) {

    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(fileToWrite);
    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    MyParquetOutputFile out = new MyParquetOutputFile(fos);
    ParquetWriter<GenericData.Record> writer = null;
    try {
      writer = AvroParquetWriter.<GenericData
          .Record>builder(out)
          .withSchema(schema)
          .withConf(new Configuration())
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .build();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return writer;
  }

  public void writeDataBaseRecordsToParquetFile(
      String fileToWrite,
      Schema schema,
      String query,
      DBTestCaseEntity entity,
      GridColumn[] columnDetails,
      Connection connection,
      DBMetaDataLoader dbMetaDataLoader) {
    ParquetWriter<GenericData.Record> writer = buildParquetWriter(fileToWrite, schema);
    // Connection connection = dbConnectionFactory.openDBConnection(entity.getDburl(), entity.getDbusername(), entity.getDbpassword(),
    // entity.getSchemaname());
    Map<String, Map<String, Integer>> columnDataTypeMap =
        buildTableColumnsSQLDataTypeMap(columnDetails, connection, entity.getSchemaname(), dbMetaDataLoader);

    try {
      if (query != null && !query.trim().isEmpty())
        writeTableData(query, schema, writer, connection, columnDetails, columnDataTypeMap, entity);
      writer.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // dbConnectionFactory.closeConnection();

  }

  public Map<String, Map<String, Integer>> buildTableColumnsSQLDataTypeMap(
      GridColumn[] columnDetails,
      Connection connection,
      String schemaName,
      DBMetaDataLoader dbMetaDataLoader) {
    Map<String, Map<String, Integer>> map = new HashMap<String, Map<String, Integer>>();
    for (int i = 0; i < columnDetails.length; i++) {
      GridColumn column = columnDetails[i];
      if (column.getMappedscpocolumn() == null)
        continue;
      String s[] = column.getMappedscpocolumn().split("\\.");
      String tableName = s[0];
      if (map.get(tableName) == null)
        map.put(tableName, dbMetaDataLoader.getColumnsDataTypesMap(schemaName, tableName, connection));
    }
    return map;
  }

  public void writeTableData(
      String searchQuery,
      Schema schema,
      ParquetWriter<GenericData.Record> writer,
      Connection conn,
      GridColumn[] columnDetails,
      Map<String, Map<String, Integer>> map,
      DBTestCaseEntity entity)
      throws IOException {
    ResultSet rs = null;
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      rs = stmt.executeQuery(searchQuery);
      if ("safetystock".equalsIgnoreCase(schema.getName())) {
        writeSafetyStockParquet(schema, rs, writer, columnDetails, map, entity);
      } else if ("procurementcalendartimes".equalsIgnoreCase(schema.getName())) {
        writeProcurementCalendarTimesParquet(rs, schema, writer);
      } else {
        while (rs.next()) {

          writeRecord(schema, rs, writer, columnDetails, map, entity);

        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        rs.close();
        stmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

  }

  private static void writeRecord(
      Schema schema,
      ResultSet rs,
      ParquetWriter<GenericData.Record> writer,
      GridColumn[] columnDetails,
      Map<String, Map<String, Integer>> map,
      DBTestCaseEntity entity)
      throws SQLException, IOException {

    GenericData.Record record = new GenericData.Record(schema);
    String key = null;
    for (int i = 0; i < columnDetails.length; i++) {
      GridColumn column = columnDetails[i];
      key = column.getColumnName();


      if (column.getMappedscpocolumn() != null) {
        String oracleTableName = column.getMappedscpocolumn().split("\\.")[0];
        String oracleColumnName = column.getMappedscpocolumn().split("\\.")[1];
        System.out.println("table : " + oracleTableName + " column : " + oracleColumnName);

        if ("aggregated_order_projection_period_upto".equalsIgnoreCase(key)) {
          Date startDate = rs.getDate("STARTDATE");
          int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()).getTotalSeconds();
          Long startDateInMicros = ((startDate.getTime() + offset * 1000) * 1000);
          Long duration = rs.getLong(oracleColumnName) * 60 * 1000 * 1000;
          Long endDateInMinutes = startDateInMicros + duration;
          record.put(key, endDateInMinutes);
          continue;
        }

        if ("SKU".equalsIgnoreCase(oracleTableName) && "ORDER_DATE_UPTO".equalsIgnoreCase(key)) {
          Date startDate = rs.getDate("OHPOST");
          int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()).getTotalSeconds();
          Long startDateInMicros = ((startDate.getTime() + offset * 1000) * 1000);
          Long duration = FALLBACK_DAYS * 1440 * 60 * 1000 * 1000;
          Long orderDateUpto = startDateInMicros + duration;
          record.put(key, orderDateUpto);
          continue;
        }

        Map<String, Integer> columnsMap = map.get(oracleTableName);
        int sqlType = columnsMap.get(oracleColumnName);

        if (sqlType == Types.TINYINT || sqlType == Types.SMALLINT || sqlType == Types.INTEGER) {
          Integer value = rs.getInt(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.BIGINT || sqlType == Types.NUMERIC) {
          Long value = rs.getLong(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
          Boolean value = rs.getBoolean(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.REAL || sqlType == Types.DECIMAL || sqlType == Types.FLOAT) {
          Float value = rs.getFloat(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.DOUBLE) {
          Double value = rs.getDouble(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.CHAR
            || sqlType == Types.VARCHAR
            || sqlType == Types.LONGVARCHAR
            || sqlType == Types.LONGNVARCHAR
            || sqlType == Types.NVARCHAR
            || sqlType == Types.NCHAR) {
          String value = rs.getString(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.DATE) {
          Date value = rs.getDate(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.TIME) {
          Time value = rs.getTime(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.TIMESTAMP) {
          Timestamp value = rs.getTimestamp(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else if (sqlType == Types.BLOB
            || sqlType == Types.BINARY
            || sqlType == Types.VARBINARY
            || sqlType == Types.LONGVARBINARY) {
          Byte value = rs.getByte(oracleColumnName);
          putToRecord(column.getColumnDataType(), record, value, key);
        } else {
        }
      } else {
        if ("ORDER_DATE_FROM".equalsIgnoreCase(key)) {
          record.put(key, ORDER_DATE_FROM * 60 * 1000 * 1000);
          continue;
        }
        if ("ORDER_DATE_UPTO".equalsIgnoreCase(key)) {
          record.put(key, ORDER_DATE_UPTO * 60 * 1000 * 1000);
          continue;
        }
        if("L_LY_ID_TARGET".equalsIgnoreCase(key)) {
          record.put(key, 1000);
          continue;
        }
        if("VOP_CU_ID".equalsIgnoreCase(key)) {
          record.put(key, 1840);
          continue;
        }
        if("VOP_UNIT_COST".equalsIgnoreCase(key)) {
          record.put(key, 1);
          continue;
        }
        record.put(key, null);
      }

    }
    writer.write(record);
  }

  private static void writeSafetyStockParquet(
      Schema schema,
      ResultSet rs,
      ParquetWriter<GenericData.Record> writer,
      GridColumn[] columnDetails,
      Map<String, Map<String, Integer>> map,
      DBTestCaseEntity entity)
      throws IOException, SQLException {

    String prevItem = null;
    String prevLoc = null;
    Float prevQty = (float) 0.0;
    Date prevEff = null;

    while (rs.next()) {
      GenericData.Record record = new GenericData.Record(schema);
      String item = rs.getString("ITEM");
      String loc = rs.getString("LOC");
      Float qty = rs.getFloat("QTY");
      Date eff = rs.getDate("EFF");

      if (prevItem == null) {
        prevItem = item;
        prevLoc = loc;
        prevQty = qty;
        prevEff = eff;
        continue;
      }

      if (item.equalsIgnoreCase(prevItem) && loc.equalsIgnoreCase(prevLoc)) {
        putToRecord(STRING_TYPE, record, prevItem, "P_EXTERNAL_CODE");
        record.put("EXTERNAL_SUPPLIER_EXTERNAL_CODE", null);
        putToRecord(STRING_TYPE, record, prevLoc, "SUPPLIER_EXTERNAL_CODE");
        putToRecord(TIMESTAMP_TYPE, record, prevEff, "EFFECTIVE_FROM");
        putToRecord(TIMESTAMP_TYPE, record, eff, "EFFECTIVE_UPTO");
        putToRecord(DOUBLE_TYPE, record, prevQty, "SAFETY_STOCK_PER_DAY");

      } else {
        putToRecord(STRING_TYPE, record, prevItem, "P_EXTERNAL_CODE");
        record.put("EXTERNAL_SUPPLIER_EXTERNAL_CODE", null);
        putToRecord(STRING_TYPE, record, prevLoc, "SUPPLIER_EXTERNAL_CODE");
        putToRecord(TIMESTAMP_TYPE, record, prevEff, "EFFECTIVE_FROM");
        putToRecord(TIMESTAMP_TYPE, record, (LATE_DATE * 60 * 1000 * 1000), "EFFECTIVE_UPTO");
        putToRecord(DOUBLE_TYPE, record, prevQty, "SAFETY_STOCK_PER_DAY");

      }
      writer.write(record);
      prevItem = item;
      prevLoc = loc;
      prevQty = qty;
      prevEff = eff;

    }
    if (prevItem != null) {
      GenericData.Record record = new GenericData.Record(schema);

      putToRecord(STRING_TYPE, record, prevItem, "P_EXTERNAL_CODE");
      record.put("EXTERNAL_SUPPLIER_EXTERNAL_CODE", null);
      putToRecord(STRING_TYPE, record, prevLoc, "SUPPLIER_EXTERNAL_CODE");
      putToRecord(TIMESTAMP_TYPE, record, prevEff, "EFFECTIVE_FROM");
      putToRecord(TIMESTAMP_TYPE, record, (LATE_DATE * 60 * 1000 * 1000), "EFFECTIVE_UPTO");
      putToRecord(DOUBLE_TYPE, record, prevQty, "SAFETY_STOCK_PER_DAY");
      writer.write(record);
    }
  }


  public void writeProcurementCalendarTimesParquet(
      ResultSet rs,
      Schema schema,
      ParquetWriter<GenericData.Record> writer)
      throws SQLException, IOException {

    int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()).getTotalSeconds();

    while (rs.next()) {
      String orderreviewcal = rs.getString("ORDERREVIEWCAL");
      long orderplacedate = (rs.getDate("ORDERPLACEDATE").getTime() + offset * 1000) * 1000;
      long arrivdate = (rs.getDate("ARRIVDATE").getTime() + offset * 1000) * 1000;
      int needcovdur = rs.getInt("NEEDCOVDUR");
      int mincovdur = rs.getInt("MINCOVDUR");
      long maxcovdur = Math.max(needcovdur, mincovdur);
      long covDur = maxcovdur * 60 * 1000 * 1000;

      for (int i = 0; i < 10; i++) {
        long addRecordValue = i * covDur;
        GenericData.Record record = new GenericData.Record(schema);
        record.put("PCT_PC_ID", convertStringToLong(orderreviewcal));
        // PCT_FINAL_ORDER_TIME
        record.put("PCT_FINAL_ORDER_TIME", orderplacedate + addRecordValue);
        // PCT_EXPECTED_ARRIVAL
        record.put("PCT_EXPECTED_ARRIVAL", arrivdate + addRecordValue);
        // PCT_EXPECTED_AVAILABILITY
        record.put("PCT_EXPECTED_AVAILABILITY", arrivdate + addRecordValue);
        writer.write(record);
      }
      break;
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Integer value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, value.doubleValue());
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value != 0 ? true : false);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, value);
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = value / 1440;
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(RULE_TYPE)) {
      record.put(vascoColumnName, (999 + value));
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, value.longValue());
    } else if (datatype.equalsIgnoreCase(TIMESTAMP_TYPE)) {
      record.put(vascoColumnName, value);
    } else {
      System.out.printf("could not convert Integer to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Long value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, value.doubleValue());
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value != 0 ? true : false);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, value.intValue());
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = value / 1440;
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(RULE_TYPE)) {
      record.put(vascoColumnName, (999 + value));
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, value);
    } else if (datatype.equalsIgnoreCase(TIMESTAMP_TYPE)) {
      record.put(vascoColumnName, value);
    } else {
      System.out.printf("could not convert Long to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Boolean value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, value ? 1.0 : 0.0);
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, value ? 1 : 0);
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, value ? 1 : 0);
    } else {
      System.out.printf("could not convert Boolean to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Float value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, value.doubleValue());
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value != 0 ? true : false);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, value.intValue());
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = value / 1440;
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(RULE_TYPE)) {
      record.put(vascoColumnName, (999 + value.intValue()));
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, value.longValue());
    } else {
      System.out.printf("could not convert Float to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Double value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, value);
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value != 0 ? true : false);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, value.intValue());
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = value / 1440;
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(RULE_TYPE)) {
      record.put(vascoColumnName, (999 + value.intValue()));
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, value.longValue());
    } else {
      System.out.printf("could not convert Double to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, String value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value);
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, Double.parseDouble(value));
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value.equalsIgnoreCase("true") ? true : false);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, Integer.parseInt(value));
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = (Double.parseDouble(value)) / 1440;
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(RULE_TYPE)) {
      record.put(vascoColumnName, (999 + Integer.parseInt(value)));
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, convertStringToLong(value));
    } else {
      System.out.printf("could not convert String to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static long convertStringToLong(String value) {
    Long longValue = 0L;
    try {
      longValue = Long.valueOf(value);
    } catch (NumberFormatException ex) {
      longValue = generateLongCode(value);
    }
    return longValue;
  }

  private static long generateLongCode(String value) {
    if (value == null || value.trim().length() == 0) {
      return 0;
    }
    long hashcode = value.hashCode();

    if (hashcode < 0) {

      long intMaxValue = Integer.MAX_VALUE;
      hashcode = (2 * intMaxValue) + hashcode;
    }
    return hashcode;

  }

  private static void putToRecord(String datatype, GenericData.Record record, Date value, String vascoColumnName) {
    int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()).getTotalSeconds();
    long millis = (value.getTime() + (offset * 1000));
    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DATE_TYPE)) {
      record.put(vascoColumnName, millis);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, millis);
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = millis / (1000 * 60 * 1440);
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(TIMESTAMP_TYPE)) {
      record.put(vascoColumnName, millis * 1000);
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, millis * 1000);
    } else {
      System.out.printf("could not convert Date to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Time value, String vascoColumnName) {

    int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()).getTotalSeconds();
    long millis = (value.getTime() + (offset * 1000));
    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DATE_TYPE)) {
      record.put(vascoColumnName, millis);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, millis);
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = millis / (1000 * 60 * 1440);
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(TIMESTAMP_TYPE)) {
      record.put(vascoColumnName, millis * 1000);
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, millis * 1000);
    } else {
      System.out.printf("could not convert Time to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Timestamp value, String vascoColumnName) {
    int offset = ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now()).getTotalSeconds();
    long millis = (value.getTime() + (offset * 1000));
    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value.toString());
    } else if (datatype.equalsIgnoreCase(DATE_TYPE)) {
      record.put(vascoColumnName, millis);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, millis);
    } else if (datatype.equalsIgnoreCase(DAYS_TYPE)) {
      double dayValue = millis / (1000 * 60 * 1440);
      record.put(vascoColumnName, dayValue);
    } else if (datatype.equalsIgnoreCase(TIMESTAMP_TYPE)) {
      record.put(vascoColumnName, millis * 1000);
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, millis * 1000);
    } else {
      System.out.printf("could not convert Timestamp to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

  private static void putToRecord(String datatype, GenericData.Record record, Byte value, String vascoColumnName) {

    if (datatype.equalsIgnoreCase(STRING_TYPE)) {
      record.put(vascoColumnName, value);
    } else if (datatype.equalsIgnoreCase(DOUBLE_TYPE)) {
      record.put(vascoColumnName, value.doubleValue());
    } else if (datatype.equalsIgnoreCase(BOOLEAN_TYPE)) {
      record.put(vascoColumnName, value.intValue() != 0 ? 1 : 0);
    } else if (datatype.equalsIgnoreCase(INTEGER_TYPE)) {
      record.put(vascoColumnName, value.intValue());
    } else if (datatype.equalsIgnoreCase(LONG_TYPE)) {
      record.put(vascoColumnName, value.longValue());
    } else {
      System.out.printf("could not convert Byte to %s for %s ", datatype, vascoColumnName);
      record.put(vascoColumnName, null);
    }
  }

}
