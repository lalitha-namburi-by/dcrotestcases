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

public class MasterDataCreator {
    
    public static void main(String args[]) throws IOException {

        String masterdatafileName= "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/masterdata.parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(masterdatafileName);
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        
        
        String masterDataOutputFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/masterdata.parquet";
        
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        ParquetWriter<Group> writer = pw.getParquetWriter(schema,masterDataOutputFileName);
        
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            String item = simpleGroup.getString("P_EXTERNAL_CODE", 0);
            String dest = simpleGroup.getString("L_EXTERNAL_CODE_TARGET", 0);
            
            SimpleGroup newSimpleGroup = new SimpleGroup(schema);
            for (Type field : schema.getFields()) {
                String fieldName = field.getName();
                
                if("SO_QUANTITY".equals(fieldName)) {
                    newSimpleGroup.add(fieldName, 0.0); 
                    continue;
                }
                // System.out.print(fieldName);
                if (field.isPrimitive()) {
                  String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
                  //addValue(originalGroup, newSimpleGroup, fieldName, multiplierindex, fieldType);
                  ParquetDataMultiplierUtils.addValue(simpleGroup, newSimpleGroup, fieldName, fieldType);
                  // originalGroup.get
                } else {
                  System.out.println("Not coded for non primitive types");
                }
            }
            writer.write(newSimpleGroup);
            
          }
        }
        
        writer.close();
        reader.close();
    }

}
