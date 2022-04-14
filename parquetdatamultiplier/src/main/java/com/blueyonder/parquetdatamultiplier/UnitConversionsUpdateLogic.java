package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UnitConversionsUpdateLogic implements UpdateLogic {

  private Map<Long,String> productidMap;
  
  public void buildProductIdMap(ParquetFileReader reader) throws IOException {
    productidMap = new HashMap<Long,String>();
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    PageReadStore pages;
    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        Long productlongid = simpleGroup.getLong("PP_P_ID", 0);
        String productStringId = simpleGroup.getString("P_EXTERNAL_CODE", 0);
        productidMap.put(productlongid, productStringId);
      }
    }

  }
  
  @Override
  public void update(
      SimpleGroup originalgroup,
      String fieldName,
      String fieldType,
      SimpleGroup newSimpleGroup,
      int multiplierindex) {
    // TODO Auto-generated method stub
    if(productidMap != null && productidMap.size()>0) {
      if("P_P_ID_MASTER".equals(fieldName)) {
      Long productid = originalgroup.getLong("P_P_ID_MASTER", 0);
      String productstringid =productidMap.get(productid);
      String value = productstringid+"-"+multiplierindex;
      long productidHashcode = ParquetDataMultiplierUtils.generateLongCode(value);
       newSimpleGroup.append(fieldName, productidHashcode);
      }else {
        ParquetDataMultiplierUtils.addValue(originalgroup, newSimpleGroup, fieldName, fieldType);
      }
      
    }else {
      System.out.println("productidMap is not found or Empty");
    }

  }

}
