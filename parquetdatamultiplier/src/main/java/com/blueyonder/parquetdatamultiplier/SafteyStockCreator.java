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

public class SafteyStockCreator {
    public static void main(String args[]) throws IOException {

        String masterdatafileName= "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/masterdata.parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(masterdatafileName);
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        
        String safetyStockFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/safetystock.parquet";
        ParquetFileReader ssreader = pr.getParquetReader(safetyStockFileName);
        MessageType ssschema = ssreader.getFooter().getFileMetaData().getSchema();
        String ssOutputFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/safetystock.parquet";
        
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        ParquetWriter<Group> sswriter = pw.getParquetWriter(ssschema,ssOutputFileName);
        
        long ohpost = 1162166400000000L;
        long iterations = 8;
        long week = 604800000000L;
        
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            String item = simpleGroup.getString("P_EXTERNAL_CODE", 0);
            String dest = simpleGroup.getString("L_EXTERNAL_CODE_TARGET", 0);
            for(int j = 0;j<iterations;j++) {
                SimpleGroup newSimpleGroup = new SimpleGroup(ssschema);
                newSimpleGroup.add("p_external_code", item);
                newSimpleGroup.add("supplier_external_code", dest);
                long effective_from = ohpost+j*604800000000L;
                newSimpleGroup.add("effective_from", effective_from);
                long effective_upto = effective_from + week;
                newSimpleGroup.add("effective_upto", effective_upto);
                double value = 0.25+j*0.25;
                newSimpleGroup.add("safety_stock_per_day", value);
                sswriter.write(newSimpleGroup);
            }
            
            
          }
        }
        
        sswriter.close();
        reader.close();
    }

}
