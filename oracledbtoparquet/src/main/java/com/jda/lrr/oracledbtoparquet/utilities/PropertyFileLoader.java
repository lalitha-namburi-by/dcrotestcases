package com.jda.lrr.oracledbtoparquet.utilities;

import java.io.FileNotFoundException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 
 * @author 1022177
 * This class handles properties file for this application.
 *
 */
public class PropertyFileLoader {

	private static Properties props = null;
    
	/**
	 * This method loads properties into application from properties file.
	 * @param propertyFileName
	 */
	public static void loadProperties(String propertyFileName) {
		FileReader reader;
		try {
			reader = new FileReader(propertyFileName);
			props = new Properties();
			props.load(reader);
		} catch (FileNotFoundException e) {
			//TODO need to add proper logs for this error
			e.printStackTrace();
		} catch (IOException e) {
			//TODO need to add proper logs for this error
			e.printStackTrace();
		}
	}
    
	/**
	 * This method reads a List as an String and then converts it into an 
	 * arrayList on the basis of delimiter.
	 * @param propertyName
	 * @return {@link java.util.ArrayList}
	 */
	public static List<String> getPropertiesValueAsList(String propertyName) {
		String str = getPropertiesValueAsString(propertyName);
		List<String> propertyList = null;
		if (str != null) {
			//TODO Need to read this delimiter from properties file itself instead of hard-coding it.
			propertyList = Arrays.asList(str.split(","));
		}
		return propertyList;
	}

	/**
	 * This method returns value of a property from properties file.
	 * @param propertyName
	 * @return
	 */
	public static String getPropertiesValueAsString(String propertyName) {
		return props.getProperty(propertyName);
	}
	
	
	/**
	 * This method returns value of a property from properties file.
	 * if property is not found then returns the default value.
	 * @param propertyName
	 * @param defaultValue
	 * @return
	 */
	public static String getPropertiesValueAsString(String propertyName, String defaultValue) {
		return props.getProperty(propertyName, defaultValue);
	}
}
