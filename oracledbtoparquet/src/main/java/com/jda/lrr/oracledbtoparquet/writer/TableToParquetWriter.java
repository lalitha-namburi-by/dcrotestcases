package com.jda.lrr.oracledbtoparquet.writer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.jda.lrr.oracledbtoparquet.avro.AvroSchemaGenerator;
import com.jda.lrr.oracledbtoparquet.dao.OracleTableDataDao;
import com.jda.lrr.oracledbtoparquet.entity.ParquetOutputFile;
import com.jda.lrr.oracledbtoparquet.entity.TableDefination;
import com.jda.lrr.oracledbtoparquet.metadata.OracleDBMetaDataLoader;
import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;

/**
 * 
 * @author 1022177
 *
 */
public class TableToParquetWriter {

	private OracleDBMetaDataLoader dbMetaDataLoader;
	private AvroSchemaGenerator schemaGenerator;
	private OracleTableDataDao oracleTableDataDao;

	// initializing all the object classes required for the application
	public TableToParquetWriter() {
		this.dbMetaDataLoader = new OracleDBMetaDataLoader();
		this.schemaGenerator = new AvroSchemaGenerator();
		this.oracleTableDataDao = new OracleTableDataDao();
	}

	public void writeTableToParquet(TableDefination tableDefination, String fileToWrite, String schemaName)
			throws IOException {
		Schema schema;
		// generate avro schema for a table
		if (tableDefination.getColumnNames() != null) {
			schema = schemaGenerator.generateAvroSchemaForTableAndColumns(dbMetaDataLoader, schemaName,
					tableDefination.getTableName(), tableDefination.getColumnNames());
		} else {
			schema = schemaGenerator.generateAvroSchemaForTable(dbMetaDataLoader, schemaName,
					tableDefination.getTableName());
		}
		String condition = PropertyFileLoader
				.getPropertiesValueAsString("table." + tableDefination.getTableName().toLowerCase() + ".condition");

		// create Avro Parquet Writer Object
		ParquetWriter<GenericData.Record> writer = buildParquetWriterObject(fileToWrite, schema);

		if (writer != null) {
			oracleTableDataDao.writeTableDataToParquet(writer, schema, condition);
		} else {
			//TODO Add loggers to log that parquet was not created
		}
	}

	private ParquetWriter<Record> buildParquetWriterObject(String fileToWrite, Schema schema) {
		ParquetOutputFile parquetFilePath = buildOutputStreamFile(fileToWrite);
		ParquetWriter<GenericData.Record> writer = null;
		try {
			writer = AvroParquetWriter.<GenericData.Record>builder(parquetFilePath).withSchema(schema)
					.withConf(new Configuration()).withCompressionCodec(CompressionCodecName.SNAPPY)
					.withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return writer;
	}
	
	private ParquetOutputFile buildOutputStreamFile(String fileToWrite){
		return new ParquetOutputFile(getOutputStream(fileToWrite));
	}
	
	private FileOutputStream getOutputStream(String fileToWrite){
		// load the path where we need to write output into a parquet file
		String outputFileDirectory = PropertyFileLoader.getPropertiesValueAsString("parquet.file.location");
		
		//creating directory if not available
		new File(outputFileDirectory).mkdir();
		
		String completeOutputFilePath = outputFileDirectory+fileToWrite.toLowerCase()+".parquet";
		//currently building output stream for local file system
		//we can create Output Stream for Azure Also here when required
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(completeOutputFilePath);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fos;
	}
}
