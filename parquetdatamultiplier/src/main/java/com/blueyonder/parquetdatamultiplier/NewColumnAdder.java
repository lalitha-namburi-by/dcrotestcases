package com.blueyonder.parquetdatamultiplier;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class NewColumnAdder {
  
  private static final HashMap<String,Long> ordergroupidMap = new HashMap<String,Long>(){
    {
      put("OOPT-REC_10.01",3623046931L);
      put("OOPT-REC_10.02",3623970452L);
      put("OOPT-REC_10.03",3624893973L);
      put("OOPT-REC_10.04",3625817494L);
      put("OOPT-REC_10.05",3626741015L);
      put("OOPT-REC_10.06",3627664536L);
      put("OOPT-REC_10.07",3628588057L);
      put("OOPT-REC_10.08",3629511578L);
      put("OOPT-REC_10.09",3630435099L);
    }
  };

  public static void main(String args[]) throws IOException {
    String inputdir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles";
    String outputdir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/outputfiles";
    String[] testCaseList = getTestCaseList(inputdir);
    //String[] fileNames = {"aggregated_order_projections","safetystock","schedrcpts"};
     String[] fileNames = {"masterdata"};
    
    long orderplacedate = 19369440L;
    long MINUTES_IN_14_DAYS = 14 *1440;

    for (int k = 0; k < testCaseList.length; k++) {
      String testcase = testCaseList[k];
      System.out.println(testcase);
      for (int j = 0; j < fileNames.length; j++) {

        String filename = fileNames[j];
        String inputFile = inputdir +"/"
            + testcase
            + "/"
            + filename
            + ".parquet";
        ParquetReader pr = new ParquetReader();
        ParquetFileReader reader = pr.getParquetReader(inputFile);
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        String outputtestcasesdir = outputdir +"/"+ testcase + "/";
        boolean dirCreated = new File(outputtestcasesdir).mkdirs();
        String outputFile = outputtestcasesdir + filename + ".parquet";

        List<String> mappings = getMappings(filename);

        GridColumn[] gridColumnsArray = buildColumnDetails(mappings);
        
        Schema newAvroSchema = AvroSchemaGenerator.generateAvroSchema(filename, gridColumnsArray);
        MessageType newParquetSchema = new AvroSchemaConverter().convert(newAvroSchema);
        ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
        ParquetWriter<Group> writer = pw.getParquetWriter(newParquetSchema, outputFile);
        //Long orderGroupId = getOrderGroupId(inputdir,testcase);
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

          for (int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
            SimpleGroup newSimpleGroup = new SimpleGroup(newParquetSchema);
            for (Type field : schema.getFields()) {
              String fieldName = field.getName();

              /*if (field.isPrimitive()) {
                if("VOP_UNIT_COST".equalsIgnoreCase(fieldName)) {
                  newSimpleGroup.add(fieldName, 1.1D);
                }else {
                String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
                ParquetDataMultiplierUtils.addValue(simpleGroup, newSimpleGroup, fieldName, fieldType);
                }
              } else {
                System.out.println("Not coded for non primitive types");
              }*/
              
              if (field.isPrimitive()) {
                //if("PP_PUG_ID".equalsIgnoreCase(fieldName)) {
                //  newSimpleGroup.add(fieldName, 1.1D);
                //}else {
                String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
                ParquetDataMultiplierUtils.addValue(simpleGroup, newSimpleGroup, fieldName, fieldType);
                //}
              } else {
                System.out.println("Not coded for non primitive types");
              }
            }
            // newSimpleGroup.add("ORDER_DATE_FROM", 0L);
            // newSimpleGroup.add("ORDER_DATE_UPTO", 4102358400000000L);

            // if ("masterdata".equalsIgnoreCase(filename)) {
            // newSimpleGroup.add("L_LY_ID_TARGET", 1001L);
            // }
            //newSimpleGroup.add("PP_PUG_ID", orderGroupId);
              //newSimpleGroup.add("VOP_UNIT_COST", 0.0D);
              //    newSimpleGroup.add("AAR_CU_ID", 0L);
            
            long activeFrom = simpleGroup.getLong("ORDER_DATE_FROM", 0)/(60*1000000);
            long activeUpto = simpleGroup.getLong("ORDER_DATE_UPTO", 0)/(60*1000000);
            if (activeFrom > (orderplacedate + MINUTES_IN_14_DAYS)) {
              newSimpleGroup.add("SHORT_TERM_PROJECTION_FLAG", 0L);
            } else {
              newSimpleGroup.add("SHORT_TERM_PROJECTION_FLAG", 1L);
            }

            if (activeUpto <= (orderplacedate + MINUTES_IN_14_DAYS)) {
              newSimpleGroup.add("LONG_TERM_PROJECTION_FLAG", 0L);
            } else {
              newSimpleGroup.add("LONG_TERM_PROJECTION_FLAG", 1L);
            }


            //newSimpleGroup.add("PUG_LBS_ENABLED", false);
            
            
            writer.write(newSimpleGroup);

          }
        }

        writer.close();
        reader.close();
      }

    }

  }

  public static List<String> getMappings(String fileName) {
    if ("masterdata".equalsIgnoreCase(fileName)) {
      return getMasteDataMappings();
    } else if ("unitconversions".equalsIgnoreCase(fileName)) {
      return getUnitConversionsMappings();
    } else if("aggregated_order_projections".equalsIgnoreCase(fileName)){
      return getAggregatedOrderProjectionsMappings();
    } else if("safetystock".equalsIgnoreCase(fileName)){
      return getSafetyStockMappings();
    } else if("schedrcpts".equalsIgnoreCase(fileName)){
      return getSchedRcptsMappings();
    }else {
      return null;
    }
  }
  
  public static List<String> getSchedRcptsMappings(){
    String schedRcptsMappings = "H_EDLC_P_ID-R-LONG,H_EDLC_L_ID_TARGET-R-LONG,H_EDLC_EXPECTED_DELIVERY_DATE-R-TIMESTAMP,H_EDLC_QUANTITY-R-DOUBLE,PP_PUG_ID-O-LONG";
    return Arrays.asList(schedRcptsMappings.split(","));
  }
  
  public static List<String> getSafetyStockMappings(){
    String safetyStockMappings = "P_EXTERNAL_CODE-R-STRING,EXTERNAL_SUPPLIER_EXTERNAL_CODE-O-STRING,SUPPLIER_EXTERNAL_CODE-R-STRING,EFFECTIVE_FROM-R-TIMESTAMP,EFFECTIVE_UPTO-R-TIMESTAMP,SAFETY_STOCK_PER_DAY-O-DOUBLE,PP_PUG_ID-O-LONG";
    return Arrays.asList(safetyStockMappings.split(","));
  }
  
  public static List<String> getAggregatedOrderProjectionsMappings(){
    String aggregatedOrderProjectionsMappings = "P_EXTERNAL_CODE-R-STRING,SUPPLIER_EXTERNAL_CODE-R-STRING,AGGREGATED_ORDER_PROJECTION_PERIOD_FROM-R-TIMESTAMP,AGGREGATED_ORDER_PROJECTION_PERIOD_UPTO-R-TIMESTAMP,AGGREGATED_ORDER_PROJECTION_MEAN-R-DOUBLE,AGGREGATED_ORDER_PROJECTION_VARIANCE-O-DOUBLE,PP_PUG_ID-O-LONG";
    return Arrays.asList(aggregatedOrderProjectionsMappings.split(","));
  }

  public static List<String> getMasteDataMappings() {
    String masterDataMappings =
        "L_LY_ID_TARGET-R-LONG,SOP_QUANTITY_MAX-R-DOUBLE-SKUPLANNINGPARAM.MAXOH,PP_P_ID-R-LONG-ITEM.ITEM,P_EXTERNAL_CODE-R-STRING-ITEM.ITEM,P_UY_ID-R-LONG-ITEM.DEFAULTUOM,SO_DATE-R-TIMESTAMP-SKU.OHPOST,SO_QUANTITY-R-DOUBLE-SKU.OH,PP_L_ID_SOURCE-R-LONG-SOURCING.SOURCE,L_EXTERNAL_CODE_SOURCE-R-STRING-SOURCING.SOURCE,PP_L_ID_TARGET-R-LONG-SOURCING.DEST,L_EXTERNAL_CODE_TARGET-R-STRING-SOURCING.DEST,PP_PPY_ID-O-LONG-SOURCING.SOURCING,PP_PC_ID-O-LONG-ORDERGROUPPARAM.ORDERREVIEWCAL,PP_SQRR_ID-O-LONG-SOURCING.CONVENIENTROUNDINGPROFILE,PP_ACTIVE_FROM-R-TIMESTAMP-SOURCING.EFF,PP_ACTIVE_UPTO-R-TIMESTAMP-SOURCING.DISC,PP_ORDER_QUANTITY_MIN-O-DOUBLE-SOURCING.MAJORSHIPQTY,PP_ORDER_MULTIPLE-O-LONG-SOURCING.MINORSHIPQTY,PP_PUG_ID-O-LONG-SOURCING.ORDERGROUP,PP_PURCHASE_SUB_GROUP-O-STRING-SOURCING.ORDERGROUPMEMBER,PUG_TREQ_ID-R-LONG-ORDERGROUP.TRANSMODE,PUG_PROJECTED_ORDER_DURATION-O-DAYS-ORDERGROUP.PROJORDERDUR,PUG_COVERAGE_DURATION_TOLERANCE-O-DAYS-ORDERGROUPPARAM.COVDURADJTOLERANCE,PUG_AAR_ID-O-LONG-ORDERGROUPPARAM.AUTOAPPROVALPROFILE,PUG_PC_ID-R-LONG-ORDERGROUPPARAM.ORDERREVIEWCAL,PUG_LOAD_BUILD_DOWN_TOLERANCE-O-DOUBLE-ORDERGROUPPARAM.LOADBUILDADJDOWNTOLERANCE,PUG_LOAD_BUILD_UP_TOLERANCE-O-DOUBLE-ORDERGROUPPARAM.LOADBUILDADJUPTOLERANCE,PUG_LBRE_ID-O-RULE-ORDERGROUPPARAM.LOADBUILDRULE,PUG_LMRE_ID-O-RULE-ORDERGROUPPARAM.LOADMINIMUMRULE,PUG_LOAD_TOLERANCE-O-DOUBLE-ORDERGROUPPARAM.LOADTOLERANCE,PUG_UNIQUE_PRODUCT_PER_PALLET-O-BOOLEAN-ORDERGROUPPARAM.SKUPERPALLETSW,PUG_TMRE_ID-O-RULE-ORDERGROUPPARAM.TRANSMODEMINRULE,PUG_VMRE_ID-O-RULE-ORDERGROUPPARAM.VENDORMINRULE,VOP_DESIRED_COVERAGE_DURATION-O-DAYS-SKUPLANNINGPARAM.DRPCOVDUR,VOP_MAX_COVERAGE_DURATION-O-DAYS-SKUPLANNINGPARAM.MAXCOVDUR,AAR_ADDITIONAL_COVERAGE_DURATION-O-DOUBLE-AUTOAPPROVALPROFILE.MAXOPTMDUR,AAR_MAX_VEHICLE_LOAD_COUNT-O-LONG-AUTOAPPROVALPROFILE.MAXVEHICLELOADCOUNT,AAR_UY_ID-O-LONG-AUTOAPPROVALPROFILE.UOM,AAR_HISTORICAL_ORDER_COUNT-O-LONG-AUTOAPPROVALPROFILE.HISTORDERCOUNT,AAR_LOWER_THRESHOLD-O-LONG-AUTOAPPROVALPROFILE.LOWERTOLERANCE,AAR_UPPER_THRESHOLD-O-LONG-AUTOAPPROVALPROFILE.UPPERTOLERANCE,AM_ACTIVE_UPTO-R-TIMESTAMP-SKUPLANNINGPARAM.EXPDATE,ORDER_DATE_FROM-R-TIMESTAMP-SKU.OHPOST,ORDER_DATE_UPTO-R-TIMESTAMP-SKU.OHPOST,VOP_CU_ID-O-LONG,VOP_UNIT_COST-O-DOUBLE,AAR_CU_ID-O-LONG,SHORT_TERM_PROJECTION_FLAG-O-LONG,LONG_TERM_PROJECTION_FLAG-O-LONG,PUG_LBS_ENABLED-O-BOOLEAN";
    return Arrays.asList(masterDataMappings.split(","));
  }

  public static List<String> getUnitConversionsMappings() {
    String unitConversionMappings =
        "P_P_ID_MASTER-R-LONG-UOMCATEGORYCONVFACTOR.ITEM,UY_NAME_SRC-R-STRING-UOMCATEGORYCONVFACTOR.SOURCECATEGORY,UY_ID_SRC-R-LONG-UOMCATEGORYCONVFACTOR.SOURCEUOM,UY_NAME_TGT-R-STRING-UOMCATEGORYCONVFACTOR.TARGETCATEGORY,UY_ID_TGT-R-LONG-UOMCATEGORYCONVFACTOR.TARGETUOM,UC_FACTOR-R-DOUBLE-UOMCATEGORYCONVFACTOR.RATIO,ORDER_DATE_FROM-R-TIMESTAMP,ORDER_DATE_UPTO-R-TIMESTAMP";
    return Arrays.asList(unitConversionMappings.split(","));
  }

  public static GridColumn[] buildColumnDetails(List<String> mappings) {
    GridColumn[] columnDetails = new GridColumn[mappings.size()];
    int index = 0;
    for (String mapping : mappings) {
      String[] s = mapping.split("-");
      GridColumn gridColumn = new GridColumn();
      gridColumn.setColumnName(s[0]);
      boolean isNullable = s[1].charAt(0) == 'R' ? false : true;
      gridColumn.setNullable(isNullable);
      gridColumn.setColumnDataType(s[2].toLowerCase());
      if (s.length == 4) {
        gridColumn.setMappedscpocolumn(s[3]);
      }
      columnDetails[index++] = gridColumn;
    }
    return columnDetails;
  }

  /*public static String[] getTestCaseList() {
    // String[] testCaseList =
    // {"DCRO-100.01","DCRO-100.02","DCRO-100.03","DCRO-100.04","DCRO-100.05","DCRO-100.06","DCRO-100.07","DCRO-100.08","DCRO-100.09","DCRO-100.10","DCRO-100.11","DCRO-100.12","DCRO-100.13","DCRO-100.14","DCRO-100.15","DCRO-100.16","DCRO-100.17","DCRO-100.18","DCRO-100.19","DCRO-100.20","DCRO-100.21","DCRO-100.22","DCRO-100.23","DCRO-100.24","DCRO-100.25","DCRO-100.26","DCRO-100.27","DCRO-100.28","DCRO-100.29","DCRO-100.30","DCRO-100.31","DCRO-100.32","DCRO-100.33","DCRO-100.34","DCRO-100.35","DCRO-100.36","DCRO-100.37","OOPT-GAA319.01","OOPT-GAA319.03","OOPT-GAA319.05","OOPT-GAA319.06","OOPT-GAA319.07","OOPT-GAA319.09","OOPT-GAA319.11","OOPT-GAA319.13","OOPT-GAA319.14","OOPT-GAA319.15","OOPT-GAA319.17","OOPT-GAA319.18","OOPT-GAA319.22","OOPT-GEX334.01","OOPT-GEX334.02","OOPT-GEX334.03","OOPT-GEX334.04","OOPT-GEX334.05","OOPT-GEX334.06","OOPT-GEX334.07","OOPT-GEX334.08","OOPT-GEX334.09","OOPT-GEX334.10","OOPT-GEX334.11","OOPT-GEX334.12","OOPT-GEX334.13","OOPT-GEX334.14","OOPT-GOA504.01","OOPT-GOA504.03","OOPT-GOA504.05","OOPT-GOA504.07","OOPT-GOA504.08","OOPT-GOA504.09","OOPT-GOA504.10","OOPT-GOA504.11","OOPT-GOA504.12","OOPT-GOA504.14","OOPT-GOA504.15","OOPT-GOA504.16","OOPT-GOA504.19","OOPT-GOA504.21","OOPT-GOA504.23","OOPT-GOA504.25","OOPT-GOA504.27","OOPT-GOR502.01","OOPT-GOR502.02","OOPT-GOR502.03","OOPT-GOR502.04","OOPT-GOR502.05","OOPT-GOR502.06","OOPT-GOR502.07","OOPT-GOR502.08","OOPT-GOR502.09","OOPT-GOR502.10","OOPT-GOR502.11","OOPT-GOR502.12","OOPT-GOR502.13","OOPT-GOR502.14","OOPT-GOR502.15","OOPT-GOR502.16","OOPT-GOR502.17","OOPT-GOR502.18","OOPT-GOR502.19","OOPT-GOR502.20","OOPT-GOR502.21","OOPT-GOR502.22","OOPT-GOR502.23","OOPT-GOR502.25","OOPT-GOR502.27","OOPT-GSD335.01","OOPT-GSD335.02","OOPT-GSD335.04","OOPT-GSD335.06","OOPT-GSD335.07","OOPT-GSD335.08","OOPT-GSD335.10","OOPT-GSD335.15","OOPT-GSD335.16","OOPT-GSD335.17","OOPT-GSD335.18","OOPT-IGN13.01","OOPT-IGN13.02","OOPT-IGN13.03","OOPT-IGN13.04","OOPT-MBG327.01","OOPT-MBG327.02","OOPT-MBG327.03","OOPT-MBG327.04","OOPT-MBG327.05","OOPT-MBG327.06","OOPT-MBG327.07","OOPT-MBG327.08","OOPT-MBG327.09","OOPT-MBG327.10","OOPT-MBG327.11","OOPT-MBG327.12","OOPT-MBG327.13","OOPT-MBG327.14","OOPT-MBG327.15","OOPT-MBG327.16","OOPT-MBG327.17","OOPT-MBG327.18","OOPT-MBG327.19","OOPT-MBG327.20","OOPT-MBG327.21","OOPT_AAA12.01","OOPT_AAA12.02","OOPT_AAA12.03","OOPT_AAA12.04","OOPT_NDB11.01","OOPT_NDB11.02","OOPT_NDB11.03"};
    // String[] testCaseList =
    // {"OOPT-FBK414.01","OOPT-FBK414.02","OOPT-FBK414.03","OOPT-FBK414.04","OOPT-FBK414.05","OOPT-FBK414.06","OOPT-FBK414.07","OOPT-FBK414.08","OOPT-FBK414.09","OOPT-FBK414.10","OOPT-FBK414.11","OOPT-FBK414.12","OOPT-FBK414.13","OOPT-FBK414.14","OOPT-FBK414.15","OOPT-FBK414.16","OOPT-FBK414.17","OOPT-FBK414.18","OOPT-FBK414.19","OOPT-FBK414.20","OOPT-FBK414.21","OOPT-FBK414.22","OOPT_AAA12.05"};
    // ,"OOPT_AAA12.05"
    String[] testCaseList = {
        "OOPT-ARQ146.20", "OOPT-BAO139.03", "OOPT-BAO139.08", "OOPT-BAO139.09", "OOPT-BAO139.10", "OOPT-BAO139.11",
        "OOPT-BAO139.14", "OOPT-BAO139.22", "OOPT-BCS138.08", "OOPT-BCS138.10", "OOPT-BCS138.12", "OOPT-BCS138.14",
        "OOPT-BCS138.15", "OOPT-BCS138.16", "OOPT-BCS138.20", "OOPT-BCS138.21", "OOPT-BOG339.02", "OOPT-BOG339.04",
        "OOPT-BOG339.06", "OOPT-BPM158.12", "OOPT-BPM158.64", "OOPT-BPP155.26", "OOPT-Cas165.04", "OOPT-Cas165.06",
        "OOPT-Cas165.08", "OOPT-COA137.05", "OOPT-COA137.10", "OOPT-COA137.24", "OOPT-CSS509.06", "OOPT-CVP173.53",
        "OOPT-DBS162.21", "OOPT-DBS162.22", "OOPT-DBS162.24", "OOPT-DBS162.25", "OOPT-DBS162.27", "OOPT-DBS162.28",
        "OOPT-DBS162.34", "OOPT-DBS162.36", "OOPT-DEL141.07", "OOPT-DTS135.18", "OOPT-DTS135.20", "OOPT-DTS135.22",
        "OOPT-DTS135.27", "OOPT-FPE153.02", "OOPT-FPE153.11", "OOPT-FPE153.19", "OOPT-FPE153.29", "OOPT-FPE153.39",
        "OOPT-FPE153.41", "OOPT-FPI152.18", "OOPT-FPI152.19", "OOPT-GFO318.01", "OOPT-GFO318.02", "OOPT-GFO318.03",
        "OOPT-GFO318.04", "OOPT-GFO318.05", "OOPT-GFO318.06", "OOPT-GFO318.07", "OOPT-GFO318.13", "OOPT-GLS321.04",
        "OOPT-GLS321.05", "OOPT-GLS321.06", "OOPT-GLS321.10", "OOPT-GLS321.11", "OOPT-GLS321.12", "OOPT-GLS321.13",
        "OOPT-GLS321.14", "OOPT-GMO332.01", "OOPT-GMO332.05", "OOPT-GMO332.08", "OOPT-GMO332.10", "OOPT-GMO332.13",
        "OOPT-GOD507.01", "OOPT-GOD507.03", "OOPT-GOD507.06", "OOPT-GOD507.09", "OOPT-GOD507.10", "OOPT-GOD507.12",
        "OOPT-GOO333.01", "OOPT-GOO333.03", "OOPT-GOO333.05", "OOPT-GOO333.07", "OOPT-GOO333.09", "OOPT-GOO333.12",
        "OOPT-GOO333.13", "OOPT-GOO333.17", "OOPT-GOO333.18", "OOPT-GPO506.01", "OOPT-GPO506.02", "OOPT-GPO506.06",
        "OOPT-GPO506.09", "OOPT-GPO506.10", "OOPT-GSD335.11", "OOPT-GSD335.20", "OOPT-GSD335.22", "OOPT-GSD335.24",
        "OOPT-GSD335.25", "OOPT-GSI324.01", "OOPT-GSI324.02", "OOPT-GSI324.03", "OOPT-GSI324.04", "OOPT-GSI324.05",
        "OOPT-GSI324.06", "OOPT-GSI324.07", "OOPT-GSI324.10", "OOPT-GSI324.13"};
    return testCaseList;
  }*/
  
  public static String[] getTestCaseList(String dir) {
    List<Path> directories = getTestCases(dir);
    List<String> testcaseNames = new ArrayList<String>(); 
    for (Path directory : directories) {
      if (directory.toString().equalsIgnoreCase(dir))
        continue;
      System.out.println(directory.getFileName().toString());
      //System.out.println(directory.toString());
      testcaseNames.add(directory.getFileName().toString());
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

  /*public static void main(String[] args) {
    String dir = "/Users/1022177/eclipse-workspace/parquetdatamultiplier/inputfiles";
    List<Path> directories = getTestCases(dir);
    for (Path directory : directories) {
      if (directory.toString().equalsIgnoreCase(dir))
        continue;
      System.out.println(directory.getFileName().toString());
      System.out.println(directory.toString());
      String outputdir = directory.toString().replace("inputfiles", "outputfiles");
      System.out.println(outputdir);
    }
  }
  */

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
  
  private static long getOrderGroupId(String inputdir,String testcase) throws IOException {
    long purchaseGroupId = -1;
   String  masterdataFilePath = inputdir+"/"+testcase+"/"+"masterdata.parquet";
   ParquetReader pr = new ParquetReader();
   ParquetFileReader reader = pr.getParquetReader(masterdataFilePath);
   MessageType schema = reader.getFooter().getFileMetaData().getSchema();
   PageReadStore pages;
   while ((pages = reader.readNextRowGroup()) != null) {
     long rows = pages.getRowCount();
     MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
     RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

     for (int i = 0; i < rows; i++) {
       SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
       purchaseGroupId =simpleGroup.getLong("PP_PUG_ID", 0);
       break;

     }
   }
   reader.close();
   return purchaseGroupId;
   
  }

}
