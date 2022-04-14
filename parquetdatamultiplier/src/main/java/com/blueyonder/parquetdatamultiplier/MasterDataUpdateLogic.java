package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class MasterDataUpdateLogic implements UpdateLogic {

  @Override
  public void update(
      SimpleGroup originalGroup,
      String fieldName,
      String fieldType,
      SimpleGroup newSimpleGroup,
      int multiplierindex) {

    if ("P_EXTERNAL_CODE".equalsIgnoreCase(fieldName)
        || "L_EXTERNAL_CODE_SOURCE".equalsIgnoreCase(fieldName)
        || "L_EXTERNAL_CODE_TARGET".equalsIgnoreCase(fieldName)) {
      String value = (String) ParquetDataMultiplierUtils.getValue(originalGroup, fieldName, fieldType);
      value = value + "-" + multiplierindex;
      newSimpleGroup.add(fieldName, value);
    } else if ("PP_P_ID".equalsIgnoreCase(fieldName)) {
      // PP_P_ID
      String value = (String) ParquetDataMultiplierUtils.getValue(originalGroup, "P_EXTERNAL_CODE", "BINARY");
      value = value + "-" + multiplierindex;
      Long longValue = ParquetDataMultiplierUtils.generateLongCode(value);
      newSimpleGroup.add(fieldName, longValue);
    } else if ("PP_L_ID_SOURCE".equalsIgnoreCase(fieldName)) {
      // PP_L_ID_SOURCE
      String value = (String) ParquetDataMultiplierUtils.getValue(originalGroup, "L_EXTERNAL_CODE_SOURCE", "BINARY");
      value = value + "-" + multiplierindex;
      Long longValue = ParquetDataMultiplierUtils.generateLongCode(value);
      newSimpleGroup.add(fieldName, longValue);
    } else if ("PP_L_ID_TARGET".equalsIgnoreCase(fieldName)) {
      // PP_L_ID_TARGET
      String value = (String) ParquetDataMultiplierUtils.getValue(originalGroup, "L_EXTERNAL_CODE_TARGET", "BINARY");
      value = value + "-" + multiplierindex;
      Long longValue = ParquetDataMultiplierUtils.generateLongCode(value);
      newSimpleGroup.add(fieldName, longValue);
    } else if ("PP_PUG_ID".equalsIgnoreCase(fieldName)) {

      String source = ((String) ParquetDataMultiplierUtils.getValue(originalGroup, "L_EXTERNAL_CODE_SOURCE", "BINARY"))+ "-" + multiplierindex;
      String dest = ((String) ParquetDataMultiplierUtils.getValue(originalGroup, "L_EXTERNAL_CODE_TARGET", "BINARY"))+ "-" + multiplierindex;
      String transmode =
          String.valueOf((Long) ParquetDataMultiplierUtils.getValue(originalGroup, "PUG_TREQ_ID", "INT64"));
      String ordergroup = transmode + "-" + source + "-" + dest + "OG";
      Long ordergrouplongvalue = ParquetDataMultiplierUtils.generateLongCode(ordergroup);
      newSimpleGroup.add(fieldName, ordergrouplongvalue);
    } else if ("PP_PURCHASE_SUB_GROUP".equalsIgnoreCase(fieldName)) {
      String source = ((String) ParquetDataMultiplierUtils.getValue(originalGroup, "L_EXTERNAL_CODE_SOURCE", "BINARY"))+ "-" + multiplierindex;
      String dest = ((String) ParquetDataMultiplierUtils.getValue(originalGroup, "L_EXTERNAL_CODE_TARGET", "BINARY"))+ "-" + multiplierindex;
      String transmode =
          String.valueOf((Long) ParquetDataMultiplierUtils.getValue(originalGroup, "PUG_TREQ_ID", "INT64"));
      String ordergroupmember = transmode + "-" + source + "-" + dest + "OGM";
      String ordergroupmemberlongvalue = String.valueOf(ParquetDataMultiplierUtils.generateLongCode(ordergroupmember));
      newSimpleGroup.add(fieldName, ordergroupmemberlongvalue);
    } else {
      ParquetDataMultiplierUtils.addValue(originalGroup, newSimpleGroup, fieldName, fieldType);
    }
  }

}
