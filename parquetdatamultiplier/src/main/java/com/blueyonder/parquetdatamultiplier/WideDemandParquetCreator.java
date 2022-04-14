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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class WideDemandParquetCreator {

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

    String p_code;
    String l_code;
    Date date = new Date();
    Random rd = new Random();
    for (int i = 0; i < itemCount; i++) {
      p_code = "item_" + i;

      for (int k = 0; k < locCount; k++) {
        l_code = "loc_" + k;
        
        List<Double> qty_1440_dur_list = new ArrayList<Double>();
        for(int j=0;j<90;j++) {
         double qty = 5 * rd.nextDouble();
         qty_1440_dur_list.add(qty);
        }
        
        List<Double> qty_10080_dur_list = new ArrayList<Double>();
        for(int j=0;j<40;j++) {
         double qty = 15 * rd.nextDouble();
         qty_10080_dur_list.add(qty);
        }
        
        
        SimpleGroup firstSimpleGroup = new SimpleGroup(schema);
        firstSimpleGroup.add("item", p_code);
        firstSimpleGroup.add("loc", l_code);
        firstSimpleGroup.add("dur", 1440L);
        //firstSimpleGroup.add("qty", qty_1440_dur_list);
        SimpleGroup secondSimpleGroup = new SimpleGroup(schema);
        
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
            if ("item".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, p_code);
            } else if ("loc".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, l_code);
            } else if ("dur".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, c_duration);
            } else if ("qty".equalsIgnoreCase(fieldName)) {
              newSimpleGroup.add(fieldName, pred_mean);
            } 
          }
          writer.write(newSimpleGroup);
          

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
