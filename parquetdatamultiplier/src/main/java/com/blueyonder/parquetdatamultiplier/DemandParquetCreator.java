package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

public class DemandParquetCreator {

  public static void main(String[] args) throws IOException {

    String inputfilePath = "/Users/1022177/Desktop/ldedemanddataset/read/parquet_Job_4435_CorID_1378_1.pq";
    String outputfilePath =
        "/Users/1022177/Desktop/ldedemanddataset/write/ds_300_10_521_5210/parquet_job_300_10_521_5210.pq";
    ParquetReader pr = new ParquetReader();
    ParquetFileReader reader = pr.getParquetReader(inputfilePath);

    MessageType schema = getSchema(reader);

    ParquetMultiplierWriter pw = new ParquetMultiplierWriter();

    ParquetWriter<Group> writer = pw.getParquetWriter(schema, outputfilePath);
    //int skuCount = 15000;

    int itemCount = 10;
    int locCount = 521;

    SimpleGroup simpleGroup = null;
    PageReadStore pages;
    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int j = 0; j < rows;) {
        simpleGroup = (SimpleGroup) recordReader.read();
        break;

      }
      break;
    }
    String p_code;
    String l_code;
    Date date = new Date();
    Random rd = new Random();
    for (int i = 0; i < itemCount; i++) {
      p_code = "item_" + i;

      long p_id = convertStringToLong(p_code);
      for (int k = 0; k < locCount; k++) {
        l_code = "loc_" + k;
        long l_id = convertStringToLong(l_code);
        long c_date = date.getTime();
        for (int j = 0; j < 300; j++) {
          long c_duration = 1440 * 60000;
          if (j >= 90) {
             c_duration = 10080 * 60000;
          }
          double pred_mean = rd.nextDouble();
          SimpleGroup newSimpleGroup = new SimpleGroup(schema);
          for (Type field : schema.getFields()) {
            String fieldName = field.getName();
            String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
            if ("p_code".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, p_code);
            } else if ("p_id".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, p_id);
            } else if ("l_code".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, l_code);
            } else if ("l_id".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, l_id);
            } else if ("c_date".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, (c_date*1000));
            } else if ("c_duration".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, c_duration);
            } else if ("pred_mean".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, pred_mean);
            } else {
              ParquetDataMultiplierUtils.addValue(simpleGroup, newSimpleGroup, fieldName, fieldType);
            }
          }
          writer.write(newSimpleGroup);
          c_date = c_date + c_duration;

        }
      }
    }
    reader.close();
    writer.close();



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

  private static MessageType getSchema(ParquetFileReader reader) {
    return reader.getFooter().getFileMetaData().getSchema();
  }

}
