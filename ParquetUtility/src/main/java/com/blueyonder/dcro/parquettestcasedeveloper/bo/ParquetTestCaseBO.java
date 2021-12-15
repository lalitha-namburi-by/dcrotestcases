package com.blueyonder.dcro.parquettestcasedeveloper.bo;


import com.blueyonder.dcro.parquettestcasedeveloper.builder.BasicTestCaseBuilderUtil;
import com.blueyonder.dcro.parquettestcasedeveloper.database.DBMetaDataLoader;
import com.blueyonder.dcro.parquettestcasedeveloper.database.DataBaseConnectionFactory;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.DBTestCaseEntity;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.GridColumn;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.NewTestCaseForm;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.Parquet;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.ParquetGrid;
import com.blueyonder.dcro.parquettestcasedeveloper.reader.ParquetReaderUtils;
import com.blueyonder.dcro.parquettestcasedeveloper.schema.AvroSchemaGenerator;
import com.blueyonder.dcro.parquettestcasedeveloper.utils.CommonUtils;
import com.blueyonder.dcro.parquettestcasedeveloper.utils.PropertiesUtils;
import com.blueyonder.dcro.parquettestcasedeveloper.writer.GenericRecordWriter;

import org.apache.avro.Schema;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ParquetTestCaseBO {

  @Autowired
  private PropertiesUtils propUtils;

  @Autowired
  private CommonUtils commonUtils;

  @Autowired
  private ParquetReaderUtils parquetReaderUtils;

  @Autowired
  private AvroSchemaGenerator avroSchemaGenerator;

  @Autowired
  private GenericRecordWriter recordWriter;

  @Autowired
  private DataBaseConnectionFactory dbConnectionFactory;

  @Autowired
  private DBMetaDataLoader dbMetaDataLoader;

  @Autowired
  private BasicTestCaseBuilderUtil basicTestCaseBuilderUtil;

  private static final String MAPPINGS = ".mappings";

  private static final String ENTITY_NAMES = "entitynames";

  private static final String TESTCASE_DIRECTORY = "testcase.directory";

  private static final String PAFRQUET_FILE_EXTENSION = ".parquet";

  private static final String DB_URL = "dburl";
  private static final String DB_USERNAME = "dbusername";
  private static final String DB_PASSWORD = "dbpassword";
  private static final String DB_SCHEMA = "schemaname";

  private static final String ENTITY_VARS = "entityvars";

  private static final String PATH_SEPERATOR = "/";

  public ParquetGrid buildParquetGrid(String testCaseName, String gridName) {

    List<String> mappings = propUtils.getPropertiesValueAsList(gridName.toLowerCase() + MAPPINGS);
    ParquetGrid parquetGrid = new ParquetGrid(gridName, mappings.size());
    GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
    parquetGrid.addColumnDetails(columnDetails);
    String parquetFileToRead = buildParquetFilePath(testCaseName, gridName);
    buildGridData(parquetFileToRead, parquetGrid);
    return parquetGrid;

  }

  public ParquetGrid buildParquetGridUsingFilePath(String filePath) {
    String gridName = getGridNameFromFilePath(filePath);
    // TODO: currently i am dependent on mappings from application.properties.
    // for this flow we can use parquet schema itself
    List<String> mappings = propUtils.getPropertiesValueAsList(gridName.toLowerCase() + MAPPINGS);
    ParquetGrid parquetGrid = new ParquetGrid(gridName, mappings.size());
    GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
    parquetGrid.addColumnDetails(columnDetails);
    buildGridData(filePath, parquetGrid);
    return parquetGrid;

  }

  public void updateParquetUsingFilePath(String filePath, ParquetGrid grid) {
    List<String> mappings = propUtils.getPropertiesValueAsList(grid.getGridName().toLowerCase() + MAPPINGS);
    GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
    grid.addColumnDetails(columnDetails);
    Schema schema = avroSchemaGenerator.generateAvroSchema(grid);
    recordWriter.writeGridToParquetFile(filePath, schema, grid);

  }

  public String getGridNameFromFilePath(String filePath) {
    String[] folders = filePath.split(PATH_SEPERATOR);
    String fileName = folders[folders.length - 1];
    String name[] = fileName.split("\\.");
    return name[0];
  }

  public List<ParquetGrid> buildParquetGridList(String testCaseName) {
    List<String> entityNames = propUtils.getPropertiesValueAsList(ENTITY_NAMES);
    List<ParquetGrid> gridList = new ArrayList<ParquetGrid>();
    for (String entity : entityNames) {
      gridList.add(buildParquetGrid(testCaseName, entity));
    }
    return gridList;
  }

  public void writeParquetGrid(String testCaseName, ParquetGrid grid) {
    List<String> mappings = propUtils.getPropertiesValueAsList(grid.getGridName().toLowerCase() + MAPPINGS);
    GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
    grid.addColumnDetails(columnDetails);
    String parquetFileToWrite = buildParquetFilePath(testCaseName, grid.getGridName());
    Schema schema = avroSchemaGenerator.generateAvroSchema(grid);
    recordWriter.writeGridToParquetFile(parquetFileToWrite, schema, grid);

  }

  public String buildGridNamesList(List<ParquetGrid> gridList) {
    return commonUtils.buildGridNamesList(gridList);
  }

  private String buildParquetFilePath(String testCaseName, String gridName) {
    String directoryPath = propUtils.getPropertiesValueAsString(TESTCASE_DIRECTORY);
    return directoryPath + testCaseName + PATH_SEPERATOR + gridName + PAFRQUET_FILE_EXTENSION;

  }

  private void buildGridData(String parquetFilePath, ParquetGrid parquetGrid) {
    GridColumn[] columnDetails = parquetGrid.getColumnsDetail();
    Parquet parquet = null;
    try {
      parquet = parquetReaderUtils.getParquetData(parquetFilePath);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if (parquet != null) {
      List<SimpleGroup> datagroups = parquet.getData();

      for (SimpleGroup dataGroup : datagroups) {
        Object[] rowData = new Object[columnDetails.length];
        for (int i = 0; i < columnDetails.length; i++) {
          GridColumn gridColumn = columnDetails[i];

          try {
            switch (gridColumn.getColumnDataType()) {
              case "string":
                rowData[i] = dataGroup.getString(gridColumn.getColumnName(), 0);
                break;
              case "double":
              case "days":
                rowData[i] = dataGroup.getDouble(gridColumn.getColumnName(), 0);
                break;
              case "boolean":
                rowData[i] = dataGroup.getBoolean(gridColumn.getColumnName(), 0);
                break;
              case "integer":
                rowData[i] = dataGroup.getInteger(gridColumn.getColumnName(), 0);
                break;
              case "long":
              case "timestamp":
              case "date":
              case "rule":
                try {
                  rowData[i] = dataGroup.getLong(gridColumn.getColumnName(), 0);
                } catch (Exception e) {
                  rowData[i] = dataGroup.getDouble(gridColumn.getColumnName(), 0);
                }
                break;

            }
          } catch (RuntimeException ex) {

            rowData[i] = null;
          }

        }
        parquetGrid.addRowData(rowData);
      }
    }
  }

  private void buildGridDataFromAnotherTestCaseData(
      String parquetFilePath,
      ParquetGrid parquetGrid,
      NewTestCaseForm newTestCaseForm) {
    GridColumn[] columnDetails = parquetGrid.getColumnsDetail();
    Parquet parquet = null;
    try {
      parquet = parquetReaderUtils.getParquetData(parquetFilePath);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    Map<String, String> buildableValueMap =
        basicTestCaseBuilderUtil.getBuildableValueMap(newTestCaseForm, getBuildableColumnNames());
    Map<String, String> alternateColumnsMap = getAlternateColumnsMap();
    if (parquet != null) {
      List<SimpleGroup> datagroups = parquet.getData();

      for (SimpleGroup dataGroup : datagroups) {
        Object[] rowData = new Object[columnDetails.length];
        for (int i = 0; i < columnDetails.length; i++) {
          GridColumn gridColumn = columnDetails[i];
          Object value = basicTestCaseBuilderUtil
              .getBuildableColumnValue(gridColumn.getColumnName(), buildableValueMap, alternateColumnsMap);
          if (value != null) {
            rowData[i] = value;
            continue;
          }

          try {
            switch (gridColumn.getColumnDataType()) {
              case "string":
                rowData[i] = dataGroup.getString(gridColumn.getColumnName(), 0);
                break;
              case "double":
              case "days":
                rowData[i] = dataGroup.getDouble(gridColumn.getColumnName(), 0);
                break;
              case "boolean":
                rowData[i] = dataGroup.getBoolean(gridColumn.getColumnName(), 0);
                break;
              case "integer":
                rowData[i] = dataGroup.getInteger(gridColumn.getColumnName(), 0);
                break;
              case "long":
              case "timestamp":
              case "date":
              case "rule":
                try {
                  rowData[i] = dataGroup.getLong(gridColumn.getColumnName(), 0);
                } catch (Exception e) {
                  rowData[i] = dataGroup.getDouble(gridColumn.getColumnName(), 0);
                }
                break;
            }
          } catch (RuntimeException ex) {

            rowData[i] = null;
          }

        }
        parquetGrid.addRowData(rowData);
      }
    }

  }

  public synchronized void createTestCaseFromDB(DBTestCaseEntity entity) {

    List<String> entityNames = propUtils.getPropertiesValueAsList(ENTITY_NAMES);
    Connection connection = dbConnectionFactory
        .openDBConnection(entity.getDburl(), entity.getDbusername(), entity.getDbpassword(), entity.getSchemaname());
    String testCaseFolder = propUtils.getPropertiesValueAsString(TESTCASE_DIRECTORY);
    commonUtils.createTestCaseDirectoryIFNotPresent(testCaseFolder, entity.getTestcasename());
    for (String entityName : entityNames) {

      List<String> mappings = propUtils.getPropertiesValueAsList(entityName + MAPPINGS);
      GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
      ParquetGrid parquetGrid = new ParquetGrid(entityName, columnDetails.length);
      parquetGrid.addColumnDetails(columnDetails);
      Schema schema = avroSchemaGenerator.generateAvroSchema(parquetGrid);
      String fileToWrite = buildFilePath(testCaseFolder, entity.getTestcasename(), entityName);
      String query = commonUtils.fetchQuery(entityName, entity);
      recordWriter
          .writeDataBaseRecordsToParquetFile(
              fileToWrite,
              schema,
              query,
              entity,
              columnDetails,
              connection,
              dbMetaDataLoader);

    }
    dbConnectionFactory.closeConnection();
    dbMetaDataLoader.emptyDBMetaData();
  }


  public synchronized void createMultipleTestCasesFromDB() {

    List<String> entityNames = propUtils.getPropertiesValueAsList(ENTITY_NAMES);
    String dbUrl = propUtils.getPropertiesValueAsString(DB_URL);
    String dbUserName = propUtils.getPropertiesValueAsString(DB_USERNAME);
    String dbPassword = propUtils.getPropertiesValueAsString(DB_PASSWORD);
    String dbSchema = propUtils.getPropertiesValueAsString(DB_SCHEMA);

    Connection connection = dbConnectionFactory.openDBConnection(dbUrl, dbUserName, dbPassword, dbSchema);

    DBTestCaseEntity entity = new DBTestCaseEntity();
    entity.setDburl(dbUrl);
    entity.setDbusername(dbUserName);
    entity.setDbpassword(dbPassword);
    entity.setSchemaname(dbSchema);

    String testCaseFolder = propUtils.getPropertiesValueAsString(TESTCASE_DIRECTORY);
    List<String> entityVars = propUtils.getPropertiesValueAsList(ENTITY_VARS);
    for (String var : entityVars) {
      String testcaseName = "TC-"+var;
      commonUtils.createTestCaseDirectoryIFNotPresent(testCaseFolder, testcaseName);
      entity.setTestcasename(testcaseName);
      commonUtils.updateEntityQueries(entity, var);

      for (String entityName : entityNames) {

        List<String> mappings = propUtils.getPropertiesValueAsList(entityName + MAPPINGS);
        GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
        ParquetGrid parquetGrid = new ParquetGrid(entityName, columnDetails.length);
        parquetGrid.addColumnDetails(columnDetails);
        Schema schema = avroSchemaGenerator.generateAvroSchema(parquetGrid);
        String fileToWrite = buildFilePath(testCaseFolder, entity.getTestcasename(), entityName);
        String query = commonUtils.fetchQuery(entityName, entity);
        recordWriter
            .writeDataBaseRecordsToParquetFile(
                fileToWrite,
                schema,
                query,
                entity,
                columnDetails,
                connection,
                dbMetaDataLoader);

      }
    }
    dbConnectionFactory.closeConnection();
    dbMetaDataLoader.emptyDBMetaData();
  }

  private String buildFilePath(String testCaseFolder, String testCaseName, String entityName) {
    return testCaseFolder + testCaseName + PATH_SEPERATOR + entityName + PAFRQUET_FILE_EXTENSION;
  }

  public void buildBasicTestCase(NewTestCaseForm newTestCaseForm) {
    List<String> entityNames = propUtils.getPropertiesValueAsList(ENTITY_NAMES);
    String testCaseFolder = propUtils.getPropertiesValueAsString(TESTCASE_DIRECTORY);
    List<String> buildableColumnNames = getBuildableColumnNames();
    commonUtils.createTestCaseDirectoryIFNotPresent(testCaseFolder, newTestCaseForm.getNewTestCaseName());
    for (String entityName : entityNames) {
      List<String> mappings = propUtils.getPropertiesValueAsList(entityName + MAPPINGS);
      GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
      ParquetGrid parquetGrid = new ParquetGrid(entityName, columnDetails.length);
      parquetGrid.addColumnDetails(columnDetails);
      Schema schema = avroSchemaGenerator.generateAvroSchema(parquetGrid);
      String fileToWrite = buildFilePath(testCaseFolder, newTestCaseForm.getNewTestCaseName(), entityName);
      basicTestCaseBuilderUtil
          .buildBasicGrid(parquetGrid, newTestCaseForm, buildableColumnNames, getAlternateColumnsMap());
      recordWriter.writeGridToParquetFile(fileToWrite, schema, parquetGrid);
    }

  }

  private List<String> getBuildableColumnNames() {
    return propUtils.getPropertiesValueAsList("buildableColumnNames");
  }

  private Map<String, String> getAlternateColumnsMap() {
    Map<String, String> alternateColumnsMap = new HashMap<String, String>();
    List<String> columnlistHavingAlternateColumn =
        propUtils.getPropertiesValueAsList("columnlistHavingAlternateColumn");
    for (String columnName : columnlistHavingAlternateColumn) {
      String alternateColumnName = propUtils.getPropertiesValueAsString(columnName + ".alternatecolumn");
      alternateColumnsMap.put(columnName, alternateColumnName);
    }
    return alternateColumnsMap;
  }

  public void buildNewTestCaseUsingAnotherCase(NewTestCaseForm newTestCaseForm) {
    List<String> entityNames = propUtils.getPropertiesValueAsList(ENTITY_NAMES);
    String testCaseFolder = propUtils.getPropertiesValueAsString(TESTCASE_DIRECTORY);
    commonUtils.createTestCaseDirectoryIFNotPresent(testCaseFolder, newTestCaseForm.getNewTestCaseName());
    for (String entityName : entityNames) {
      List<String> mappings = propUtils.getPropertiesValueAsList(entityName + MAPPINGS);
      GridColumn[] columnDetails = commonUtils.buildColumnDetails(mappings);
      ParquetGrid parquetGrid = new ParquetGrid(entityName, columnDetails.length);
      parquetGrid.addColumnDetails(columnDetails);
      Schema schema = avroSchemaGenerator.generateAvroSchema(parquetGrid);
      String fileToWrite = buildFilePath(testCaseFolder, newTestCaseForm.getNewTestCaseName(), entityName);
      String fileToRead = buildFilePath(testCaseFolder, newTestCaseForm.getOldTestCaseName(), entityName);
      // build GridData from Another TestCase
      buildGridDataFromAnotherTestCaseData(fileToRead, parquetGrid, newTestCaseForm);
      recordWriter.writeGridToParquetFile(fileToWrite, schema, parquetGrid);
    }
  }

  public NewTestCaseForm buildBasicTestCaseCreatorForm() {
    NewTestCaseForm newTestCaseForm = new NewTestCaseForm();
    buildSuffixPrefixValueMap(newTestCaseForm);
    buildColumnDefaultValueMap(newTestCaseForm);
    return newTestCaseForm;
  }

  public NewTestCaseForm buildTestCaseCreatorFormFromAnotherTestCase(String oldTestCaseName) {
    NewTestCaseForm newTestCaseForm = new NewTestCaseForm();
    newTestCaseForm.setOldTestCaseName(oldTestCaseName);
    buildSuffixPrefixValueMap(newTestCaseForm);
    // buildColumnDefaultValueMap(newTestCaseForm);
    return newTestCaseForm;
  }

  private void buildSuffixPrefixValueMap(NewTestCaseForm newTestCaseForm) {

    List<String> buildableColumnNames = propUtils.getPropertiesValueAsList("buildableColumnNames");
    String defaultPrefix = propUtils.getPropertiesValueAsString("default.prefix", "");
    Map<String, String> suffixPrefixValueMap = new HashMap<String, String>();
    suffixPrefixValueMap.put("default.prefix", defaultPrefix);
    for (String columnName : buildableColumnNames) {
      String columnPrefixValue = propUtils.getPropertiesValueAsString(columnName + ".prefix");
      suffixPrefixValueMap.put(columnName + ".prefix", columnPrefixValue);
      String columnSuffixValue = propUtils.getPropertiesValueAsString(columnName + ".suffix");
      suffixPrefixValueMap.put(columnName + ".suffix", columnSuffixValue);
    }
    newTestCaseForm.setSuffixPrefixValueMap(suffixPrefixValueMap);
  }

  private void buildColumnDefaultValueMap(NewTestCaseForm newTestCaseForm) {

    List<String> defaultValueColumnNames = propUtils.getPropertiesValueAsList("defaultValueColumnNames");
    Map<String, String> defaultColumnValueMap = new HashMap<String, String>();
    for (String columnName : defaultValueColumnNames) {
      String defaultValue = propUtils.getPropertiesValueAsString(columnName + ".defaultvalue");
      defaultColumnValueMap.put(columnName + ".defaultvalue", defaultValue);
    }
    newTestCaseForm.setDefaultColumnValueMap(defaultColumnValueMap);
  }

}
