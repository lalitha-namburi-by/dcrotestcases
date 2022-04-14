package com.blueyonder.parquetdatamultiplier;

import org.apache.parquet.example.data.simple.SimpleGroup;

public interface UpdateLogic {

  void update(SimpleGroup originalgroup, String fieldName, String fieldType, SimpleGroup newSimpleGroup,int multiplierindex);

}
