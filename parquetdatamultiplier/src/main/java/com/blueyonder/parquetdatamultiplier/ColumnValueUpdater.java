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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnValueUpdater {
  
  public static void main(String args[]) throws IOException {
    String inputdir = "/Users/1022773/Documents/LRR/Codebases/parquetdatamultiplier/inputfiles";
    String outputdir = "/Users/1022773/Documents/LRR/Codebases/parquetdatamultiplier/outputfiles";
    String[] testCaseList = getTestCaseList(inputdir);
//    String[] fileNames = {"currencyconversions"};
     String[] fileNames = {"masterdata","buy_guide_data","autoapprovalexceptions","currencyconversions","procurementcalendartimes","purchasegroupvendorminimums","schedrcpts","shipquantityroundingrules","transportequipmentcapacity","unitconversions","safetystock","aggregated_order_projections","unapproved_orders_data","historical_orders_data","manual_orders_data","schedrcpts_supplier_data"};

     ParquetDataMultiplierUtils.updateValuesFailures = 0;
		for (int k = 0; k < testCaseList.length; k++) {
			String testcase = testCaseList[k];
			if (testcase != null && ParquetDataMultiplierUtils.isTestCase(testcase)) {
				System.out.println(testcase);
				for (int j = 0; j < fileNames.length; j++) {

					String filename = fileNames[j];
					String inputFile = inputdir + "/" + testcase + "/" + filename + ".parquet";
					ParquetReader pr = new ParquetReader();
					ParquetFileReader reader = null;
					try {
						reader = pr.getParquetReader(inputFile);
					} catch (IOException ioe) {
						System.out.println("File not found" + inputFile);
						continue;
					}
					MessageType schema = reader.getFooter().getFileMetaData().getSchema();
					String outputtestcasesdir = outputdir + "/" + testcase + "/";
					boolean dirCreated = new File(outputtestcasesdir).mkdirs();
					String outputFile = outputtestcasesdir + filename + ".parquet";

					ParquetMultiplierWriter pw = new ParquetMultiplierWriter();
					ParquetWriter<Group> writer = pw.getParquetWriter(schema, outputFile);

					PageReadStore pages;
					while (reader.getRecordCount() != 0 && ((pages = reader.readNextRowGroup()) != null)) {
						long rows = pages.getRowCount();
						MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
						RecordReader<Group> recordReader = columnIO.getRecordReader(pages,
								new GroupRecordConverter(schema));

						for (int i = 0; i < rows; i++) {
							SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
							SimpleGroup newSimpleGroup = new SimpleGroup(schema);
							for (Type field : schema.getFields()) {
								String fieldName = field.getName();

								if (field.isPrimitive()) {
									if (ParquetDataMultiplierUtils.needsUpdate(fieldName)) {
//										newSimpleGroup.add(fieldName, 2L);

										String testCaseType = ParquetDataMultiplierUtils.getTypeFromTestCase(testcase);

										String testCaseNumber = ParquetDataMultiplierUtils
												.getNumberFromTestCase(testcase);

										String testCaseSubNumber = ParquetDataMultiplierUtils
												.getSubNumberFromTestCase(testcase);

										String columnNumber = ParquetDataMultiplierUtils.getColumnNumber(fieldName);

										String prefix = testCaseType + testCaseNumber + testCaseSubNumber
												+ columnNumber;

										String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
										ParquetDataMultiplierUtils.updateValueToNewNomenclature(simpleGroup,
												newSimpleGroup, fieldName, fieldType, testcase, prefix);
//										ParquetDataMultiplierUtils.appendToExistingStringValueColumn(simpleGroup,
//												newSimpleGroup, fieldName, fieldType, testcase);
									} else {
										String fieldType = field.asPrimitiveType().getPrimitiveTypeName().name();
										ParquetDataMultiplierUtils.addValue(simpleGroup, newSimpleGroup, fieldName,
												fieldType);
									}
								} else {
									System.out.println("Not coded for non primitive types");
								}
							}

							writer.write(newSimpleGroup);

						}
					}

					writer.close();
					reader.close();
				}

			}

		}
		System.out.println("Total exceptions in changing values : " + ParquetDataMultiplierUtils.updateValuesFailures);
		ParquetDataMultiplierUtils.updateValuesFailures = 0;

  }
  
	public static String[] getTestCaseList(String dir) {
		List<Path> directories = getTestCases(dir);
		List<String> testcaseNames = new ArrayList<String>();
		for (Path directory : directories) {
			if (directory.toString().equalsIgnoreCase(dir))
				continue;
//			System.out.println(directory.getFileName().toString());
			// System.out.println(directory.toString());
			testcaseNames.add(directory.getFileName().toString());
			// String outputdir = directory.toString().replace("inputfiles", "outputfiles");
			// System.out.println(outputdir);
		}

		String[] testcaseArray = new String[testcaseNames.size()];
		int i = 0;
		for (String testcase : testcaseNames) {
			if (!testcase.equalsIgnoreCase("LongTermProjections")) {
				testcaseArray[i] = testcase;
				i++;
			}
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
