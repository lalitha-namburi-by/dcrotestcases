package com.blueyonder.dcro.parquettestcasedeveloper.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;

public class Parquet {
	private List<SimpleGroup> data;
    private List<Type> schema;
    
    public Parquet() {
    	this.data = new ArrayList<SimpleGroup>();
    	this.schema = new ArrayList<Type>();
    }
    
    public Parquet(List<SimpleGroup> data, List<Type> schema) {
        this.data = data;
        this.schema = schema;
    }

    public List<SimpleGroup> getData() {
        return data;
    }

    public List<Type> getSchema() {
        return schema;
    }
    
    public boolean addDataToParquet(List<SimpleGroup> dataList) {
    	return this.data.addAll(dataList);
    }
    
    public String toString() {
    	StringBuilder outputString = new StringBuilder();
    	for(Type type : this.schema) {
    		outputString.append("SCHEMA : ");
    		outputString.append("/n");
    		outputString.append(type.toString());
    		outputString.append("/n");
    	}
    	for(SimpleGroup sgp : this.data) {
    		outputString.append("DATA : ");
    		outputString.append("/n");
    		outputString.append(sgp.toString());
    		outputString.append("/n");
    	}
    	return outputString.toString();
    }

	public boolean addSchemaToParquet(List<Type> fields) {
		if(this.schema.isEmpty()) {
			return this.schema.addAll(fields);
		}
		TreeSet<Type> originalField = new TreeSet<Type>(this.schema);
		TreeSet<Type> newField = new TreeSet<Type>(fields);
		return originalField.equals(newField);
	}
 }
