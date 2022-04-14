package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class SSUpdateLogic implements UpdateLogic {
    
    @Override
    public void update(
        SimpleGroup originalgroup,
        String fieldName,
        String fieldType,
        SimpleGroup newSimpleGroup,
        int multiplierindex) {

      if ("p_external_code".equals(fieldName)
          || "supplier_external_code".equals(fieldName)) {
        String value = (String) ParquetDataMultiplierUtils.getValue(originalgroup, fieldName, fieldType);
        value = value + "-" + multiplierindex;
        newSimpleGroup.add(fieldName, value);
      } else {
        ParquetDataMultiplierUtils.addValue(originalgroup, newSimpleGroup, fieldName, fieldType);
      }
    }

}
