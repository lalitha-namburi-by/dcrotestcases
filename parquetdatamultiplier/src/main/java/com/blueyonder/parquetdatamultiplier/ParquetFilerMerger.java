package com.blueyonder.parquetdatamultiplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ParquetFilerMerger {

  public static void main(String[] args) throws IOException {
    String inputdir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles";
    String outputdir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles";
    String[] testCaseList = getTestCaseList(inputdir);
    String[] fileNames = {"unitconversions","unapproved_orders_data","transportequipmentcapacity","shipquantityroundingrules","schedrcpts","safetystock","masterdata","autoapprovalexceptions","procurementcalendartimes","purchasegroupvendorminimums"};
    ParquetWriter<Group> writer = null;
    MessageType schema = null;
    for (int j = 0; j < fileNames.length; j++) {
      String filename = fileNames[j];
      for (int k = 0; k < testCaseList.length; k++) {
        String testcase = testCaseList[k];
        String inputFile = inputdir + "/" + testcase + "/" + filename + ".parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(inputFile);

        if (k == 0) {
          schema = reader.getFooter().getFileMetaData().getSchema();
          String outputtestcasesdir = outputdir + "/" + "mergedfiles" + "/";
          boolean dirCreated = new File(outputtestcasesdir).mkdirs();
          String outputFile = outputtestcasesdir + filename + ".parquet";
          writer = buildWriter(schema, getOutputFile(outputFile), 3 * 1024);
        }

        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            writer.write(simpleGroup);
          }
        }
        reader.close();
      }
      writer.close();
    }
  }


  private static ParquetWriter<Group> buildWriter(MessageType schema, OutputFile file, int rowGroupSize) {
    ParquetWriter<Group> writer = null;
    try {
      writer = ExampleParquetWriter
          .builder(file)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withType(schema)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .withConf(new Configuration())
          .withRowGroupSize(rowGroupSize)
          .build();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return writer;
  }


  private static OutputFile getOutputFile(String fileToWrite) {
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

  public static String[] getTestCaseList(String dir) {
    List<Path> directories = getTestCases(dir);
    List<String> testcaseNames = new ArrayList<String>();
    for (Path directory : directories) {
      if (directory.toString().equalsIgnoreCase(dir))
        continue;
      System.out.println(directory.getFileName().toString());
      testcaseNames.add(directory.getFileName().toString());
    }

    String[] testcaseArray = new String[testcaseNames.size()];
    int i = 0;
    for (String testcase : testcaseNames) {
      testcaseArray[i] = testcase;
      i++;
    }
    return testcaseArray;
  }

  public static List<Path> getTestCases(String dir) {
    Path directory = Paths.get(dir);
    List<Path> directories = null;
    try {
      directories = Files.walk(directory).filter(Files::isDirectory).collect(Collectors.toList());
    } catch (IOException e) {
      // process exception
    }
    Collections.sort(directories);
    return directories;

  }

}
