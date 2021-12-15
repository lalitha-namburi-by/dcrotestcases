package com.blueyonder.dcro.parquettestcasedeveloper.utils;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class PropertiesUtils {
	
	@Autowired
	private Environment env;
	
	/**
	 * This method reads a List as an String and then converts it into an 
	 * arrayList on the basis of delimiter.
	 * @param propertyName
	 * @return {@link java.util.ArrayList}
	 */
	public  List<String> getPropertiesValueAsList(String propertyName) {
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
	public  String getPropertiesValueAsString(String propertyName) {
		return env.getProperty(propertyName);
	}
	
	
	/**
	 * This method returns value of a property from properties file.
	 * if property is not found then returns the default value.
	 * @param propertyName
	 * @param defaultValue
	 * @return
	 */
	public  String getPropertiesValueAsString(String propertyName, String defaultValue) {
		return env.getProperty(propertyName, defaultValue);
	}

}
