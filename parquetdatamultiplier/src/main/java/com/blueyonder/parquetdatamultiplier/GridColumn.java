package com.blueyonder.parquetdatamultiplier;

public class GridColumn {
  
  private String columnName;
  
  private String columnDataType;
  
  private Object defaultValue;
  
  private boolean isNullable;
  
  private String mappedscpocolumn;

  public String getMappedscpocolumn() {
      return mappedscpocolumn;
  }

  public void setMappedscpocolumn(String mappedscpocolumn) {
      this.mappedscpocolumn = mappedscpocolumn;
  }

  public boolean isNullable() {
      return isNullable;
  }

  public void setNullable(boolean isNullable) {
      this.isNullable = isNullable;
  }

  public String getColumnName() {
      return columnName;
  }

  public void setColumnName(String columnName) {
      this.columnName = columnName;
  }

  public String getColumnDataType() {
      return columnDataType;
  }

  public void setColumnDataType(String columnDataType) {
      this.columnDataType = columnDataType;
  }

  public Object getDefaultValue() {
      return defaultValue;
  }

  public void setDefaultValue(Object defaultValue) {
      this.defaultValue = defaultValue;
  }

}
