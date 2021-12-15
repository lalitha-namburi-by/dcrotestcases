package com.blueyonder.dcro.parquettestcasedeveloper.entities;

import java.util.HashMap;
import java.util.Map;

public class NewTestCaseForm {
	
	private String newTestCaseName;
	
	private String oldTestCaseName;
	
	private Map<String, String> suffixPrefixValueMap = new HashMap<String, String>();

	private Map<String, String> defaultColumnValueMap = new HashMap<String, String>();
	
	public String getNewTestCaseName() {
		return newTestCaseName;
	}

	public void setNewTestCaseName(String newTestCaseName) {
		this.newTestCaseName = newTestCaseName;
	}

	public String getOldTestCaseName() {
		return oldTestCaseName;
	}

	public void setOldTestCaseName(String oldTestCaseName) {
		this.oldTestCaseName = oldTestCaseName;
	}

	
	
	public Map<String, String> getSuffixPrefixValueMap() {
		return suffixPrefixValueMap;
	}

	public void setSuffixPrefixValueMap(Map<String, String> suffixPrefixValueMap) {
		this.suffixPrefixValueMap = suffixPrefixValueMap;
	}


	public Map<String, String> getDefaultColumnValueMap() {
		return defaultColumnValueMap;
	}

	public void setDefaultColumnValueMap(Map<String, String> defaultColumnValueMap) {
		this.defaultColumnValueMap = defaultColumnValueMap;
	}

}
