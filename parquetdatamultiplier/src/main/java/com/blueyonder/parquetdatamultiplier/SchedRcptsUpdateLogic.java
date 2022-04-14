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

public class SchedRcptsUpdateLogic implements UpdateLogic {

    private Map<Long,String> productidMap;
    private Map<Long,String> destidMap;
    
    public void buildProductIdMap(ParquetFileReader reader) throws IOException {
        productidMap = new HashMap<Long, String>();
        destidMap = new HashMap<Long, String>(); 
        MessageType schema = reader.getFooter()
            .getFileMetaData()
            .getSchema();
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            for (int i = 0; i < rows; i++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                Long productlongid = simpleGroup.getLong("PP_P_ID", 0);
                String productStringId = simpleGroup.getString("P_EXTERNAL_CODE", 0);
                Long destLongId = simpleGroup.getLong("PP_L_ID_TARGET", 0);
                String destStringId = simpleGroup.getString("L_EXTERNAL_CODE_TARGET", 0);
                productidMap.put(productlongid, productStringId);
                destidMap.put(destLongId, destStringId);
            }
        }

    }

    @Override
    public void update(SimpleGroup originalgroup, String fieldName, String fieldType, SimpleGroup newSimpleGroup, int multiplierindex) {
        // TODO Auto-generated method stub

        if (productidMap != null && productidMap.size() > 0 && destidMap != null && destidMap.size() > 0) {
            if ("H_EDLC_P_ID".equals(fieldName)) {
                Long productid = originalgroup.getLong("H_EDLC_P_ID", 0);
                String productstringid = productidMap.get(productid);
                String value = productstringid + "-" + multiplierindex;
                long productidHashcode = ParquetDataMultiplierUtils.generateLongCode(value);
                newSimpleGroup.append(fieldName, productidHashcode);
            } else if ("H_EDLC_L_ID_TARGET".equals(fieldName)) {
                Long destId = originalgroup.getLong("H_EDLC_L_ID_TARGET", 0);
                String destStringId = destidMap.get(destId);
                String value = destStringId + "-" + multiplierindex;
                long destidHashcode = ParquetDataMultiplierUtils.generateLongCode(value);
                newSimpleGroup.append(fieldName, destidHashcode);
            } else {
                ParquetDataMultiplierUtils.addValue(originalgroup, newSimpleGroup, fieldName, fieldType);
            }

        } else {
            System.out.println("productidMap is not found or Empty");
        }

    }

}
