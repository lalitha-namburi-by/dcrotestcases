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
import java.util.HashSet;

public class UnitConversionsCreator {
    
    public static void main(String args[]) throws IOException {

        HashSet<String> unitconversions = new HashSet<String>();
        String masterdatafileName= "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/masterdata.parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(masterdatafileName);
        
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        
        String ucFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles/unitconversions.parquet";
        ParquetFileReader ucreader = pr.getParquetReader(ucFileName);
        MessageType ucschema = ucreader.getFooter().getFileMetaData().getSchema();
        String outputFileName = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles/unitconversions.parquet";
        
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        ParquetWriter<Group> ucwriter = pw.getParquetWriter(ucschema,outputFileName);
        
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            Long itemCode = simpleGroup.getLong("PP_P_ID", 0);
            Long defaultUomCode = simpleGroup.getLong("P_UY_ID", 0);
            String key = itemCode+""+defaultUomCode;
            if(unitconversions.contains(key)) {
                continue;
            }
            unitconversions.add(key);
            SimpleGroup newSimpleGroup = new SimpleGroup(ucschema);
            newSimpleGroup.add("P_P_ID_MASTER", itemCode);
            newSimpleGroup.add("UY_NAME_SRC",String.valueOf(defaultUomCode));
            newSimpleGroup.add("UY_ID_SRC",defaultUomCode);
            newSimpleGroup.add("UY_NAME_TGT","50001");
            newSimpleGroup.add("UY_ID_TGT", 50001L);
            newSimpleGroup.add("UC_FACTOR", 1.0);
            
            ucwriter.write(newSimpleGroup);
            
            
            
            SimpleGroup newSimpleGroup1 = new SimpleGroup(ucschema);
            newSimpleGroup1.add("P_P_ID_MASTER", itemCode);
            newSimpleGroup1.add("UY_NAME_SRC",String.valueOf(defaultUomCode));
            newSimpleGroup1.add("UY_ID_SRC",defaultUomCode);
            newSimpleGroup1.add("UY_NAME_TGT","50002");
            newSimpleGroup1.add("UY_ID_TGT", 50002L);
            newSimpleGroup1.add("UC_FACTOR", 2.0);
            
            ucwriter.write(newSimpleGroup1);
            
            SimpleGroup newSimpleGroup2 = new SimpleGroup(ucschema);
            newSimpleGroup2.add("P_P_ID_MASTER", itemCode);
            newSimpleGroup2.add("UY_NAME_SRC",String.valueOf(defaultUomCode));
            newSimpleGroup2.add("UY_ID_SRC",defaultUomCode);
            newSimpleGroup2.add("UY_NAME_TGT","50003");
            newSimpleGroup2.add("UY_ID_TGT", 50003L);
            newSimpleGroup2.add("UC_FACTOR", 2.0);
            
            ucwriter.write(newSimpleGroup2);
            
          }
        }
        
        ucreader.close();
        ucwriter.close();
        reader.close();
    
        
    }

}
