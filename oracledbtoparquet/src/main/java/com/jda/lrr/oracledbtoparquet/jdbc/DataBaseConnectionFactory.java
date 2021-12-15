package com.jda.lrr.oracledbtoparquet.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.jda.lrr.oracledbtoparquet.utilities.PropertyFileLoader;

/**
 * This class manages database connection.
 * @author 1022177
 *
 */
public class DataBaseConnectionFactory {

	private static Connection connection = null;

	/**
	 * creates a connection with the database if it doesn't exist already.
	 * @return {@link java.sql.Connection}
	 */
	public static Connection getDBConnection() {

		if (connection == null) {

			// load url, username and password for the databse from properties file 
			String url = PropertyFileLoader.getPropertiesValueAsString("db.url");
			String username = PropertyFileLoader.getPropertiesValueAsString("db.username");
			String password = PropertyFileLoader.getPropertiesValueAsString("db.password");

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
				connection = DriverManager.getConnection(url, username, password);
				
			} catch (SQLException sqlex) {
				sqlex.printStackTrace();
			}
		}
		return connection;
	}

	/**
	 * closes the connection with the database.
	 */
	public static void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
