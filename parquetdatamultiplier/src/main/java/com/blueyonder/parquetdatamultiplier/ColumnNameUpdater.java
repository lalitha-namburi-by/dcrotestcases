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
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnNameUpdater {

  private static final Map<String, String> orderheaderMap = new HashMap<String, String>() {
    {
      put("forceoptimize", "optimizforcedsw");
    }
  };
  
  private static final Map<String, String> orderheaderhistMap = new HashMap<String, String>() {
    {
      put("forceoptimize", "optimizforcedsw");
    }
  };
  
  private static final Map<String, String> lrr_fb_orderheaderMap = new HashMap<String, String>() {
    {
      put("forceoptimize", "optimizforcedsw");
    }
  };

  private static final Map<String, Map<String, String>> alternatColumnMaps =
      new HashMap<String, Map<String, String>>() {
        {
          put("orderheader", orderheaderMap);
          put("orderheaderhist",orderheaderhistMap);
          put("lrr_fb_orderheader",lrr_fb_orderheaderMap);
        }
      };

  public static void main(String args[]) throws IOException {
    String[] fileNames = {
        "orderheader", "orderheaderhist","lrr_fb_orderheader"};
    String input_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles";
    String output_dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles";

   /* String[] folderArray = {
        "DCRO-100.01", "DCRO-100.02", "DCRO-100.03", "DCRO-100.04", "DCRO-100.05", "DCRO-100.06", "DCRO-100.07",
        "DCRO-100.08", "DCRO-100.09", "DCRO-100.10", "DCRO-100.11", "DCRO-100.12", "DCRO-100.13", "DCRO-100.14",
        "DCRO-100.15", "DCRO-100.16", "DCRO-100.17", "DCRO-100.18", "DCRO-100.19", "DCRO-100.20", "DCRO-100.21",
        "DCRO-100.22", "DCRO-100.23", "DCRO-100.24", "DCRO-100.25", "DCRO-100.26", "DCRO-100.27", "DCRO-100.28",
        "DCRO-100.29", "DCRO-100.30", "DCRO-100.31", "DCRO-100.32", "DCRO-100.33", "DCRO-100.34", "DCRO-100.35",
        "DCRO-100.36", "DCRO-100.37", "OOPT-GAA319.03", "OOPT-GAA319.07", "OOPT-GAA319.09", "OOPT-GAA319.11",
        "OOPT-GAA319.13", "OOPT-GAA319.14", "OOPT-GAA319.15", "OOPT-GAA319.17", "OOPT-GEX334.01", "OOPT-GEX334.02",
        "OOPT-GEX334.03", "OOPT-GEX334.04", "OOPT-GEX334.05", "OOPT-GEX334.09", "OOPT-GEX334.10", "OOPT-GSD335.01",
        "OOPT-GSD335.02", "OOPT-GSD335.04", "OOPT-GSD335.06", "OOPT-GSD335.07", "OOPT-GSD335.08", "OOPT-GSD335.10",
        "OOPT-GSD335.15", "OOPT-GSD335.16", "OOPT-GSD335.17", "OOPT-GSD335.18", "OOPT-MBG327.01", "OOPT-MBG327.02",
        "OOPT-MBG327.03", "OOPT-MBG327.04", "OOPT-MBG327.05", "OOPT-MBG327.06", "OOPT-MBG327.07", "OOPT-MBG327.08",
        "OOPT-MBG327.09", "OOPT-MBG327.10", "OOPT-MBG327.11", "OOPT-MBG327.12", "OOPT-MBG327.13", "OOPT-MBG327.14",
        "OOPT-MBG327.15", "OOPT-MBG327.16", "OOPT-MBG327.17", "OOPT-MBG327.18", "OOPT-MBG327.19", "OOPT-MBG327.20",
        "OOPT-MBG327.21"};*/
    String[] folderArray = getTestCaseList(input_dir); 
    for (int i = 0; i < folderArray.length; i++) {
      String folder = folderArray[i];
      String inputDirPath = input_dir +"/"+ folder + "/";
      String outputDirPath = output_dir+"/" + folder + "/";
      for (String filename : fileNames) {

        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = null;
        try {
         reader = pr.getParquetReader(inputDirPath + filename + ".parquet");
        }catch(FileNotFoundException fio) {
          continue;
          
        }
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        MessageType schema = getSchema(reader);
        boolean dirCreated = new File(outputDirPath).mkdir();
        Map<String, String> newColumnNameMap = alternatColumnMaps.get(filename);
        MessageType newSchema = buildNewSchema(schema, newColumnNameMap);
        // newSchema.toString();
        ParquetWriter<Group> writer = pw.getParquetWriter(newSchema, outputDirPath + filename + ".parquet");

        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int j = 0; j < rows; j++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            SimpleGroup newSimmpleGroup = copyData(simpleGroup, schema, newSchema, newColumnNameMap);
            writer.write(newSimmpleGroup);

          }
        }
        reader.close();
        writer.close();

      }
    }
  }

  private static MessageType buildNewSchema(MessageType oldSchema, Map<String, String> newColumnNameMap) {
    String newSchemaString = oldSchema.toString();
    for (Type field : oldSchema.getFields()) {
      String fieldName = field.getName();
      String newFieldName = newColumnNameMap.get(fieldName);
      if (newFieldName != null) {
        newSchemaString = newSchemaString.replace(fieldName, newFieldName);
      }

    }
    MessageType newSchema = MessageTypeParser.parseMessageType(newSchemaString);
    return newSchema;
  }

  private static SimpleGroup copyData(
      SimpleGroup simpleGroup,
      MessageType oldSchema,
      MessageType newSchema,
      Map<String, String> newColumnNameMap) {
    SimpleGroup newSimpleGroup = new SimpleGroup(newSchema);
    for (Type field : oldSchema.getFields()) {
      String fieldName = field.getName();
      String newFieldName = newColumnNameMap.get(fieldName);
      if(newFieldName == null) {
        newFieldName = fieldName;
      }
      if (field.isPrimitive()) {
        String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
        copyValue(simpleGroup, newSimpleGroup, fieldName, newFieldName, fieldType);
      } else {

      }
    }
    return newSimpleGroup;
  }

  private static void copyValue(
      SimpleGroup originalGroup,
      SimpleGroup newSimpleGroup,
      String oldFieldName,
      String newFieldName,
      String fieldType) {
    try {
      if ("BINARY".equals(fieldType)) {
        String value = originalGroup.getString(oldFieldName, 0);
        newSimpleGroup.add(newFieldName, value);
      } else if ("DOUBLE".equals(fieldType)) {
        double value = (Double) originalGroup.getDouble(oldFieldName, 0);
        newSimpleGroup.add(newFieldName, value);
      } else if ("INT64".equals(fieldType)) {
        long value = (Long) originalGroup.getLong(oldFieldName, 0);
        newSimpleGroup.add(newFieldName, value);
      } else if ("BOOLEAN".equals(fieldType)) {
        boolean value = (Boolean) originalGroup.getBoolean(oldFieldName, 0);
        newSimpleGroup.add(newFieldName, value);
      } else {
        System.out.println("No matching type found");
      }
    } catch (Exception ex) {
      System.out.println("Exception caught");
    }

  }

  private static MessageType getSchema(ParquetFileReader reader) {
    return reader.getFooter().getFileMetaData().getSchema();
  }
  
  public static String[] getTestCaseList(String dir) {
    List<Path> directories = getTestCases(dir);
    List<String> testcaseNames = new ArrayList<String>(); 
    for (Path directory : directories) {
      if (directory.toString().equalsIgnoreCase(dir))
        continue;
      String testcase_dir =  directory.toString();
      String testcase_folder = testcase_dir.replace(dir+"/", "");
      
      System.out.println(testcase_folder);
      //System.out.println(directory.getFileName().toString());
      //System.out.println(directory.toString());
      //testcaseNames.add(directory.getFileName().toString());
      testcaseNames.add(testcase_folder);
      //String outputdir = directory.toString().replace("inputfiles", "outputfiles");
      //System.out.println(outputdir);
    }
    
    String[] testcaseArray = new String[testcaseNames.size()];
    int i = 0;
    for(String testcase : testcaseNames ) {
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
