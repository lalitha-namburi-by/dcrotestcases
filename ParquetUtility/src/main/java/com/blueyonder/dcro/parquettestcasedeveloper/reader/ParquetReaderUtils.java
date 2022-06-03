package com.blueyonder.dcro.parquettestcasedeveloper.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.springframework.stereotype.Component;

import com.blueyonder.dcro.parquettestcasedeveloper.entities.Parquet;

@Component
public class ParquetReaderUtils {
	
  public Parquet getParquetData(String filePath) throws IOException {
    ParquetFileReader reader = getParquetReader(filePath);
    if (reader != null && reader.getRecordCount() > 0) {
      List<SimpleGroup> simpleGroups = getSimpleGroupData(reader);
      List<Type> fields = getFields(reader);
      reader.close();
      return new Parquet(simpleGroups, fields);
    }
    return null;
  }
	
	public List<SimpleGroup> getSimpleGroupData(ParquetFileReader reader) throws IOException{
		List<SimpleGroup> simpleGroups = new ArrayList<>();
		MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		PageReadStore pages;
		while ((pages = reader.readNextRowGroup()) != null) {
			long rows = pages.getRowCount();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
			RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

			for (int i = 0; i < rows; i++) {
				SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
				simpleGroups.add(simpleGroup);
			}
		}
		return simpleGroups;
	}
	
	public List<Type> getFields(ParquetFileReader reader) throws IOException{
		MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		List<Type> fields = schema.getFields();
		return fields;
	}
	
	public ParquetFileReader getParquetReader(String filePath) throws IOException{
		ParquetFileReader reader = ParquetFileReader
				.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
		return reader;
	}

}
