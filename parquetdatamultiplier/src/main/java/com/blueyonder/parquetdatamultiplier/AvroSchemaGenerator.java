package com.blueyonder.parquetdatamultiplier;


import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class AvroSchemaGenerator {
  
  public static Schema generateAvroSchema(String entityName,GridColumn[] gridColumnArray) {

      StringBuilder sb = new StringBuilder();
      sb.append("{").append("\n");
      sb.append("\t").append("\"type\"").append("\t").append(":").append("\t").append("\"record\",").append("\n");
      sb.append("\t").append("\"name\"").append("\t").append(":").append("\t").append("\"")
              .append(entityName).append("\",").append("\n");
      sb.append("\t").append("\"fields\"").append("\t").append(":").append("\t").append("[ ").append("\n");

      // iterating through all the required columns and converting them in
      // fields

      int size = gridColumnArray.length;
      int i = 0;
      
      for (int j = 0 ;j<size;j++) {
          GridColumn gridColumn = gridColumnArray[j];
          // opening field
          sb.append("\t").append("{").append("\n");

          // Appending the field Name to avro schema field
          sb.append("\t").append("\t").append("\"name\"").append("\t").append(":").append("\"")
                  .append(gridColumn.getColumnName()).append("\",").append("\n");

          sb.append("\t").append("\t");

          // Appending the field Type to avro Schema field
          sb.append("\"type\" ").append(":");

          // creating Union type if field is nullable
          if (gridColumn.isNullable()) {
              sb.append(" [").append("\"null\"").append(", ");
          }

          if (getAvroLogicalType(gridColumn.getColumnDataType()) != null) {
              sb.append(" {");
              sb.append("\"type\" ").append(":");
          }
          
          sb.append("\"").append(getAvroType(gridColumn.getColumnDataType())).append("\"");

          if (getAvroLogicalType(gridColumn.getColumnDataType()) != null) {
              sb.append(",").append("\"logicalType\"").append(" : ").append("\"")
                      .append(getAvroLogicalType(gridColumn.getColumnDataType())).append("\"");
          }

          if (getAvroLogicalType(gridColumn.getColumnDataType()) != null) {
              sb.append(" }");
          }

          // closing Union type if field is nullable
          if (gridColumn.isNullable()) {
              sb.append("]");
          }

          // Appending default value to avro schema field if field is nullable
          if (gridColumn.isNullable()) {
              sb.append(",").append("\n");
              sb.append("\t").append("\t").append("\"default\"").append(" : ").append("null");
          }

          // closing the field
          sb.append("\n");
          sb.append("\t").append("}");

          if (i != size - 1) {
              sb.append(",").append("\n");
              i++;
          }
      }

      sb.append("]").append("\n");
      sb.append("}");

      System.out.println(sb.toString());
      return new Schema.Parser().parse(sb.toString());
      
  }

  private static String getAvroLogicalType(String dataType) {

      if ("DATE".equalsIgnoreCase(dataType)) {
          return LogicalTypes.date().getName();
      } else if ("TIME".equalsIgnoreCase(dataType)) {
          return LogicalTypes.timeMicros().getName();
      } else if ("TIMESTAMP".equalsIgnoreCase(dataType)) {
          return LogicalTypes.timestampMicros().getName();
      } else {
          return null;
      }

  }

  private static String getAvroType(String dataType) {
      return toAvroType(dataType).getName();
  }

  /**
   * 
   * @param sqlType
   * @return {@link org.apache.avro.Schema.Type}
   */
  private static Type toAvroType(String dataType) {
      switch (dataType.toUpperCase()) {

      case "TINYINT":
      case "SMALLINT":
      case "INTEGER":
          return Type.INT;

      case "BIGINT":
      case "NUMERIC":
      case "RULE":
      case "LONG":  
          return Type.LONG;

      case "BIT":
      case "BOOLEAN":
          return Type.BOOLEAN;

      case "REAL":
          return Type.FLOAT;

      case "DECIMAL":
      case "FLOAT":
      case "DOUBLE":
      case "DAYS":
          return Type.DOUBLE;

      case "CHAR":
      case "VARCHAR":
      case "LONGVARCHAR":
      case "LONGNVARCHAR":
      case "NVARCHAR":
      case "NCHAR":
      case "STRING":
          return Type.STRING;

      case "DATE":
          //return Type.INT;
      case "TIME":
      case "TIMESTAMP":
          return Type.LONG;

      case "BLOB":
      case "BINARY":
      case "VARBINARY":
      case "LONGVARBINARY":
          return Type.BYTES;

      default:
          throw new IllegalArgumentException("Cannot convert Data type " + dataType);
      }
  }

}
