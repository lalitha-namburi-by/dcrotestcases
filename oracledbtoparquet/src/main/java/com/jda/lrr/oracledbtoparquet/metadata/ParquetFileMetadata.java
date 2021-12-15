package com.jda.lrr.oracledbtoparquet.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetFileMetadata {

	public Schema getParquetMetadata(Path targetFilePath) throws IOException {
		System.out.println("parquet file path : " + targetFilePath.toUri().getPath());

		FileSystem fs = targetFilePath.getFileSystem(new Configuration());
		ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(targetFilePath)
				.withConf(new Configuration()).build();

		String schemaString = reader.read().getSchema().toString();
		Schema schema = reader.read().getSchema();

		/*
		 * Another way to get parquet metadata ParquetMetadata footer =
		 * ParquetFileReader.readFooter(fs.getConf(), targetFilePath); FileMetaData fmd
		 * = footer.getFileMetaData();
		 */
		reader.close();

		return schema;

	}

}
