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

public class SchedRcptsCreator {
    
    public static void main(String args[]) throws IOException {


        String masterdatafileName= "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/masterdata.parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(masterdatafileName);
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        
        String schedRcptFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/schedrcpts.parquet";
        ParquetFileReader srreader = pr.getParquetReader(schedRcptFileName);
        MessageType srschema = srreader.getFooter().getFileMetaData().getSchema();
        String srOutputFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/schedrcpts.parquet";
        
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        ParquetWriter<Group> sswriter = pw.getParquetWriter(srschema,srOutputFileName);
        
        long ohpost = 1162166400000000L;
        long iterations = 2;
        long day = 86400000000L;
        
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            long item = simpleGroup.getLong("PP_P_ID", 0);
            long dest = simpleGroup.getLong("PP_L_ID_TARGET", 0);
            for(int j = 0;j<iterations;j++) {
                SimpleGroup newSimpleGroup = new SimpleGroup(srschema);
                newSimpleGroup.add("H_EDLC_P_ID", item);
                newSimpleGroup.add("H_EDLC_L_ID_TARGET", dest);
                long delivery_date = ohpost+(2*j+1)*day;
                newSimpleGroup.add("H_EDLC_EXPECTED_DELIVERY_DATE", delivery_date);
                double value = 0.25+j*0.25;
                newSimpleGroup.add("H_EDLC_QUANTITY", value);
                sswriter.write(newSimpleGroup);
            }
            
            
          }
        }
        
        sswriter.close();
        reader.close();
    
        
    }

}
