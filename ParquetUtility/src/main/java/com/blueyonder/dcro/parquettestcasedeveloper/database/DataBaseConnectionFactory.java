package com.blueyonder.dcro.parquettestcasedeveloper.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.stereotype.Repository;

@Repository
public class DataBaseConnectionFactory {
	
	private static Connection connection = null;
	
	public Connection openDBConnection(String dburl, String username, String password, String schemaName) {

		if (connection == null) {

			// Loading or registering Oracle JDBC driver class
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			} catch (ClassNotFoundException cnfex) {
				System.out.println("Problem in" + " loading Oracle JDBC driver");
				cnfex.printStackTrace();
			}

			// Step 2: Opening database connection
			try {

				// get connection using DriverManager class
				connection = DriverManager.getConnection(dburl, username, password);
				
			} catch (SQLException sqlex) {
				sqlex.printStackTrace();
			}

		}

		return connection;


	}
	
	/**
	 * closes the connection with the database.
	 */
	public  void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
				connection = null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}


