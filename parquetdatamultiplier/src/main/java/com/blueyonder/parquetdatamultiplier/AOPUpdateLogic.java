package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class AOPUpdateLogic implements UpdateLogic {

  @Override
  public void update(
      SimpleGroup originalgroup,
      String fieldName,
      String fieldType,
      SimpleGroup newSimpleGroup,
      int multiplierindex) {

    if ("P_EXTERNAL_CODE".equalsIgnoreCase(fieldName)
        || "supplier_external_code".equalsIgnoreCase(fieldName)) {
      String value = (String) ParquetDataMultiplierUtils.getValue(originalgroup, fieldName, fieldType);
      value = value + "-" + multiplierindex;
      newSimpleGroup.add(fieldName, value);
    } else {
      ParquetDataMultiplierUtils.addValue(originalgroup, newSimpleGroup, fieldName, fieldType);
    }
  }

}
