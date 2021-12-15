package com.jda.lrr.oracledbtoparquet.reader;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import com.jda.lrr.oracledbtoparquet.dao.OracleTableDataInsert;
import com.jda.lrr.oracledbtoparquet.entity.ParquetColDef;
import com.jda.lrr.oracledbtoparquet.entity.TableDefination;
import com.jda.lrr.oracledbtoparquet.entity.TablesDefinationsListBuilder;
import com.jda.lrr.oracledbtoparquet.jdbc.DataBaseConnectionFactory;
import com.jda.lrr.oracledbtoparquet.metadata.ParquetFileMetadata;
import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;

/**
 * This class handles reading generic data records from a parquet file.
 * 
 * @author 1022177
 *
 */
public class ParquetDataReader {

	private ParquetFileMetadata parquetMetadata;
	private OracleTableDataInsert oracleTableDataInsert;

	public ParquetDataReader() {

	}

	public ParquetDataReader(ParquetFileMetadata parquetMetadata, OracleTableDataInsert oracleTableDataInsert) {
		this.parquetMetadata = parquetMetadata;
		this.oracleTableDataInsert = oracleTableDataInsert;
	}

	public void readMultipleParquet() throws Exception {

		String outputFileDirectory = PropertyFileLoader.getPropertiesValueAsString("parquet.file.location");
		// Path parquetFilePath = new Path(outpath);

		// Load schemaName from properties file
		String schemaName = PropertyFileLoader.getPropertiesValueAsString("db.schema.name");

		// build TablesDefinationsList to write to parquet file
		TablesDefinationsListBuilder tablesDefinationsListBuilder = new TablesDefinationsListBuilder();
		List<TableDefination> tablesDefinationsList = tablesDefinationsListBuilder.getTablesDefinationsList();

		for (TableDefination tableDefination : tablesDefinationsList) {
			String outputFilePath = outputFileDirectory + tableDefination.getTableName().toLowerCase() + ".parquet";
			Path parquetFilePath = new Path(outputFilePath);
			readFromParquet(parquetFilePath);
		}

		// close the connection with the oracle database
		DataBaseConnectionFactory.closeConnection();

	}

	/**
	 * This method reads generic data records from a parquet file.
	 * 
	 * @param filePathToRead
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void readFromParquet(Path filePathToRead) throws Exception {

		// parsing the schema
		Schema schema = parquetMetadata.getParquetMetadata(filePathToRead);

		List<String> columnNames = new ArrayList<>();
		// Column Def list build
		List<ParquetColDef> colDefList = parquetColDefListBuilder(schema, columnNames);

		// Set table defination
		TableDefination tableDef = new TableDefination();
		String tableName = schema.getName();
		tableDef.setTableName(tableName);
		tableDef.setColumnNames(columnNames);

		// get data generic records from parquet file
		ParquetDataReader parquetDataReader = new ParquetDataReader();
		List<GenericData.Record> tableData = parquetDataReader.getGenericRecords(filePathToRead);

		// Inserting data to oracle database
		oracleTableDataInsert.insertDataToDB(schema, tableDef, colDefList, tableData);

	}

	public List<GenericData.Record> getGenericRecords(Path filePathToRead) throws Exception {
		List<GenericData.Record> recordData = new ArrayList<>();
		try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(filePathToRead)
				.withConf(new Configuration()).build()) {

			GenericData.Record record;

			while ((record = reader.read()) != null) {
				recordData.add(record);
			}
		}
		return recordData;
	}

	public List<ParquetColDef> parquetColDefListBuilder(Schema schema, List<String> columnNames) {
		List<Field> fields = schema.getFields();
		List<ParquetColDef> colDefList = new ArrayList<>();

		System.out.println("schema= " + schema);

		for (Field field : fields) {
			ParquetColDef colDef = new ParquetColDef();
			String columnName = field.name();
			Schema sch = field.schema();
			Type mainType = field.schema().getType();

			columnNames.add(columnName);
			if (mainType == Schema.Type.UNION) {
				String typ = field.schema().getTypes().get(1).getName();

				if (field.schema().getTypes().get(1).getLogicalType() != null) {
					String LogTyp = field.schema().getTypes().get(1).getLogicalType().getName();
					System.out.println(" logical Data type = " + LogTyp);
					String logicalType = colDef.getOracleLogicalType(LogTyp);
					System.out.println("converted SQL logical type = " + logicalType);
					colDef.setColumnName(columnName);
					colDef.setSqlDataType(logicalType);
					colDef.setNullable(false);
				} else {
					String sqlDataType = field.schema().getTypes().get(1).getType().getName();
					Type isNullable = field.schema().getTypes().get(0).getType();
					colDef.setColumnName(columnName);
					colDef.setSqlDataType(sqlDataType);
					if (isNullable.toString().equalsIgnoreCase(null)) {
						colDef.setNullable(true);
					}
				}

			} else {
				if (mainType == Schema.Type.LONG) {

					String dateType = field.schema().getType().getName();

					if (field.schema().getLogicalType() != null) {
						String LogTyp = field.schema().getLogicalType().getName();
						System.out.println("logical Data type = " + LogTyp);
						String logicalType = colDef.getOracleLogicalType(LogTyp);
						System.out.println("converted SQL logical type = " + logicalType);
						colDef.setColumnName(columnName);
						colDef.setSqlDataType(logicalType);
						colDef.setNullable(false);
					} else {
						colDef.setColumnName(columnName);
						colDef.setSqlDataType(dateType);
						colDef.setNullable(false);
					}
				} else {
					String dataType = mainType.getName();
					colDef.setColumnName(columnName);
					colDef.setSqlDataType(dataType);
					colDef.setNullable(false);
				}
			}

			colDefList.add(colDef);
		}
		return colDefList;

	}
}
