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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class Filter {

    
    public static void main( String[] args ) throws IOException
    {
        String masterdatafileName= "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/masterdata.parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(masterdatafileName);
        
        HashSet<String> masterDataset = new HashSet<String>();
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            String item = simpleGroup.getString("P_EXTERNAL_CODE", 0);
            String dest = simpleGroup.getString("L_EXTERNAL_CODE_TARGET", 0);
            String key = item+"@"+dest;
            masterDataset.add(key);
            
          }
        }
        
        reader.close();
        
        long maxDate = 1178236800000000L;
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        String aopfileName= "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/aggregated_order_projections.parquet";
        String aopOutputFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/aggregated_order_projections.parquet";
        ParquetFileReader aopreader = pr.getParquetReader(aopfileName);
        MessageType aopschema = aopreader.getFooter().getFileMetaData().getSchema();
        ParquetWriter<Group> writer = pw.getParquetWriter(aopschema,aopOutputFileName);
        PageReadStore aoppages;
        while ((aoppages = aopreader.readNextRowGroup()) != null) {
          long rows = aoppages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(aopschema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(aoppages, new GroupRecordConverter(aopschema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            String item = simpleGroup.getString("p_external_code", 0);
            String dest = simpleGroup.getString("supplier_external_code", 0);
            
            String key = item+"@"+dest;
            if(masterDataset.contains(key)) {
               long date = simpleGroup.getLong("aggregated_order_projection_period_from", 0);
               if(date<maxDate) {
                  writer.write(simpleGroup); 
               }
            }
            
            
          }
        }
        aopreader.close();
        writer.close();
        
    }
}
