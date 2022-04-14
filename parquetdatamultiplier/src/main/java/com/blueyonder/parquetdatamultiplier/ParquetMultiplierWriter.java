package com.blueyonder.parquetdatamultiplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class ParquetMultiplierWriter {

  public ParquetWriter<Group> getParquetWriter(MessageType schema,String filrToWrite){
    return buildWriter(schema,getOutputFile(filrToWrite));
  }

  private ParquetWriter<Group> buildWriter(MessageType schema, OutputFile file) {
    ParquetWriter<Group> writer = null;
    try {
      writer = ExampleParquetWriter
          .builder(file)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withType(schema)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .withConf(new Configuration())
          .build();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return writer;
  }
  
  
  private OutputFile getOutputFile(String fileToWrite) {
    FileOutputStream fos = null;
    try {
        fos = new FileOutputStream(fileToWrite);
    } catch (FileNotFoundException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
    }
    MyParquetOutputFile outputFile = new MyParquetOutputFile(fos);
    return outputFile;
  }
}
