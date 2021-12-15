package com.jda.lrr.oracledbtoparquet;

import java.io.IOException;
import java.util.List;

import com.jda.lrr.oracledbtoparquet.avro.AvroSchemaGenerator;
import com.jda.lrr.oracledbtoparquet.dao.OracleTableDataDao;
import com.jda.lrr.oracledbtoparquet.dao.OracleTableDataInsert;
import com.jda.lrr.oracledbtoparquet.metadata.OracleDBMetaDataLoader;
import com.jda.lrr.oracledbtoparquet.metadata.ParquetFileMetadata;
import com.jda.lrr.oracledbtoparquet.reader.ParquetDataReader;
import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;
import com.jda.lrr.oracledbtoparquet.writer.ParquetDataWriter;

/**
 * 
 * @author 1022177 
 * This application provides following functionalities: 
 * 1. It loads data from oracle tables and write it into parquet file for the
 * tables mentioned in properties file.
 * 
 */
public class ParquetReaderWriterWithAvroApp {

	// TODO Need to configure loggers for this application and add proper logs
	// for this application
	// TODO Need to configure proper exception handling for the application
	// TODO Need to add Junit test cases for this application.
	
	// declaring object classes required for the application
		private ParquetFileMetadata parquetMetadata;
		private OracleTableDataInsert oracleTableDataInsert;

		// initializing all the object classes required for the application
		private ParquetReaderWriterWithAvroApp() {
			this.parquetMetadata = new ParquetFileMetadata();
			this.oracleTableDataInsert = new OracleTableDataInsert();
		}

	/**
	 * This is the main method of application where execution starts.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		String propertyFile = args[0];

		// let's load the properties
		PropertyFileLoader.loadProperties(propertyFile);

		// let's get the operations which we want to perform i.e. export,import
		List<String> operations = PropertyFileLoader.getPropertiesValueAsList("operationstoperform");

		ParquetReaderWriterWithAvroApp parquetReaderWriterWithAvroApp = new ParquetReaderWriterWithAvroApp();
		for (String operation : operations) {
			if ("import".equalsIgnoreCase(operation)) {
				parquetReaderWriterWithAvroApp.importDataFromParquet();
			} else if ("export".equalsIgnoreCase(operation)) {
				parquetReaderWriterWithAvroApp.exportDataToParquet();
			} else {
				System.out.println("Please provide a correct operation");
			}
		}
	}

	/**
	 * This method exports the oracle db data to parquet files.
	 */
	private void exportDataToParquet() {
		ParquetDataWriter parquetDataWriter = new ParquetDataWriter();
		try {
			parquetDataWriter.writeMultipleTablesToParquetFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void importDataFromParquet() {
		ParquetDataReader parquetDataReader = new ParquetDataReader(parquetMetadata, oracleTableDataInsert);
		try {
			parquetDataReader.readMultipleParquet();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
