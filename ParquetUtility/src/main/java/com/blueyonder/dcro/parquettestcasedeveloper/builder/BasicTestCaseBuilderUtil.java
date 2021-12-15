package com.blueyonder.dcro.parquettestcasedeveloper.builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.blueyonder.dcro.parquettestcasedeveloper.entities.GridColumn;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.NewTestCaseForm;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.ParquetGrid;

@Component
public class BasicTestCaseBuilderUtil {

	public Map<String, String> getBuildableValueMap(NewTestCaseForm newTestCaseForm,
			List<String> buildableColumnNames) {

		Map<String, String> buildableValueMap = new HashMap<>();
		for (String buildableColumn : buildableColumnNames) {
			String suffixValue = getSuffixValue(buildableColumn, newTestCaseForm.getSuffixPrefixValueMap());
			if (suffixValue != null) {
				String prefixValue = getPrefixValue(buildableColumn, newTestCaseForm.getSuffixPrefixValueMap());
				String keyValue = prefixValue + "-" + newTestCaseForm.getNewTestCaseName() + "-" + suffixValue;
				buildableValueMap.put(buildableColumn, keyValue);
			}
		}
		return buildableValueMap;

	}

	public String getDefaultValueforColumn(String columnName, Map<String, String> defaultColumnValueMap) {
		return defaultColumnValueMap.get(columnName + ".defaultvalue");
	}

	private String getPrefixValue(String columnName, Map<String, String> suffixPrefixValueMap) {
		String prefixValue = suffixPrefixValueMap.get(columnName + ".prefix");
		if (prefixValue == null || prefixValue.isEmpty()) {
			prefixValue = suffixPrefixValueMap.get("default.prefix");
		}
		return prefixValue;

	}

	private String getSuffixValue(String columnName, Map<String, String> suffixPrefixValueMap) {
		return suffixPrefixValueMap.get(columnName + ".suffix");
	}

	public void buildBasicGrid(ParquetGrid grid, NewTestCaseForm newTestCaseForm, List<String> buildableColumnNames,
			Map<String, String> alternateColumnNames) {
		GridColumn[] columnDetails = grid.getColumnsDetail();
		Object[] rowData = new Object[columnDetails.length];
		Map<String, String> buildableValueMap = getBuildableValueMap(newTestCaseForm, buildableColumnNames);
		for (int i = 0; i < columnDetails.length; i++) {
			GridColumn gridColumn = columnDetails[i];
			rowData[i] = getColumnValue(gridColumn, buildableValueMap, newTestCaseForm.getDefaultColumnValueMap(),
					alternateColumnNames);

		}
		grid.addRowData(rowData);
	}

	private String getAlternateColumnName(String columnName, Map<String, String> alternateColumnNames) {
		return alternateColumnNames.get(columnName);
	}

	private Object getColumnValue(GridColumn gridColumn, Map<String, String> buildableValueMap,
			Map<String, String> defaultColumnValueMap, Map<String, String> alternateColumnNames) {
		String columnName = gridColumn.getColumnName();
		Object value = getBuildableColumnValue(columnName, buildableValueMap, alternateColumnNames);

		if (value == null) {
			value = getDefaultValueforColumn(columnName, defaultColumnValueMap);
		}

		if (value == null && !gridColumn.isNullable()) {
			value = getDefaultValueAsPerType(gridColumn.getColumnDataType());
		}
		return value;

	}

	public Object getBuildableColumnValue(String columnName, Map<String, String> buildableValueMap,
			Map<String, String> alternateColumnNames) {
		Object value = buildableValueMap.get(columnName);
		String alternateColumnName = getAlternateColumnName(columnName, alternateColumnNames);
		if (alternateColumnName != null) {
			value = buildableValueMap.get(alternateColumnName);
		}
		return value;
	}

	private Object getDefaultValueAsPerType(String columnType) {
		Object value = null;
		switch (columnType) {
		case "string":
			value = "";
			break;
		case "double":
		case "days":
			value = "0.0";
			break;
		case "boolean":
			value = "false";
			break;
		case "long":
                case "timestamp":
                case "date":
                case "rule":
			value = "0";
			break;
		case "integer":
			value = "0";

		}
		return value;
	}
}
