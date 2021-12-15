package com.jda.lrr.oracledbtoparquet.writer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.jda.lrr.oracledbtoparquet.entity.TableDefination;
import com.jda.lrr.oracledbtoparquet.entity.TablesDefinationsListBuilder;
import com.jda.lrr.oracledbtoparquet.jdbc.DataBaseConnectionFactory;
import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;

/**
 * This class handles writing Generic Data records of a table to parquet file.
 * 
 * @author 1022177
 *
 */
public class ParquetDataWriter {

	/**
	 * This method loads all the tables which we need to export to parquet.
	 * Creates multi-threaded tasks for each table and adds them to thread executor.
	 * @throws IOException
	 */
	public void writeMultipleTablesToParquetFiles() throws IOException {
		
		String numberOfThreads = PropertyFileLoader.getPropertiesValueAsString("numberofthreads");
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.valueOf(numberOfThreads));
		// Load schemaName from properties file
		String schemaName = PropertyFileLoader.getPropertiesValueAsString("db.schema.name");

		// build TablesDefinationsList to write to parquet file
		TablesDefinationsListBuilder tablesDefinationsListBuilder = new TablesDefinationsListBuilder();
		List<TableDefination> tablesDefinationsList = tablesDefinationsListBuilder.getTablesDefinationsList();

		// Iterate through all the table definations and write their data into
		// parquet file.
		for (TableDefination tableDefination : tablesDefinationsList) {
			Runnable task = new Runnable() {		               
                @Override
                public void run() {
                	try {
                		String outputFileName =   tableDefination.getTableName();
            			TableToParquetWriter ttpw = new TableToParquetWriter();
                		ttpw.writeTableToParquet(tableDefination, outputFileName, schemaName);
    				}
                	catch (Exception e) {
						System.out.println(e.getMessage());						}
                }
            };
           executor.execute(task);
			
		}
		
		executor.shutdown();

		// close the connection with the oracle database
		DataBaseConnectionFactory.closeConnection();
	}
}
