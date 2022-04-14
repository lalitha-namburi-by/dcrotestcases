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

public class DataMultiplier {

  private ParquetFileReader reader;
  private ParquetWriter<Group> writer;
  private int multiplierConstant;
  private MessageType schema;
  private UpdateLogic updateLogic;

  public DataMultiplier(ParquetFileReader reader, ParquetWriter<Group> writer, int multiplierConstant,UpdateLogic updateLogic) {
    this.reader = reader;
    this.writer = writer;
    this.multiplierConstant = multiplierConstant;
    this.schema = reader.getFooter().getFileMetaData().getSchema();
    this.updateLogic = updateLogic;
  }

  public void multiplyData() throws IOException {
    PageReadStore pages;
    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        multiplyAndWrite(simpleGroup);
      }
    }

  }

  private void multiplyAndWrite(SimpleGroup group) throws IOException {
    for (int i = 0; i < this.multiplierConstant; i++) {
      SimpleGroup sg = createMultipliedGroup(group, i);
      this.writer.write(sg);

    }
  }

  private SimpleGroup createMultipliedGroup(SimpleGroup originalGroup, int multiplierindex) {
    SimpleGroup newSimpleGroup = new SimpleGroup(this.schema);
    if (multiplierindex > 0) {
      for (Type field : schema.getFields()) {
        String fieldName = field.getName();
        // System.out.print(fieldName);
        if (field.isPrimitive()) {
          String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
          //addValue(originalGroup, newSimpleGroup, fieldName, multiplierindex, fieldType);
          this.updateLogic.update(originalGroup, fieldName, fieldType, newSimpleGroup, multiplierindex);
          // originalGroup.get
        } else {
          System.out.println("Not coded for non primitive types");
        }
        // field.
        // field.

      }
    } else {
      newSimpleGroup = originalGroup;
    }
    return newSimpleGroup;

  }

}
