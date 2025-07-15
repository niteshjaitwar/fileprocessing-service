package com.adp.esi.digitech.file.processing.generator.util;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.Row;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CSVGeneratorUtils {
	
	private JSONObject outputFileRule;
	private boolean isTransformationRequired;
	private String delimeter;
	private String rowSeparator;
	private String encoding;
	private boolean includeHeader;
	private JSONArray fields;
	private String dataSetName;
	
	@Autowired(required = true)
	public CSVGeneratorUtils(JSONObject outputFileRule, String isTransformationRequired) {
		this.outputFileRule = outputFileRule;
		this.isTransformationRequired = "Y".equalsIgnoreCase(isTransformationRequired);
		this.delimeter = outputFileRule.optString("fieldSeperator", ",");
		this.rowSeparator = outputFileRule.optString("rowSeparator", "\n");
		this.encoding = outputFileRule.optString("encoding", "UTF-8");
		this.includeHeader = "Y".equalsIgnoreCase(outputFileRule.optString("includeHeader", "N"));
		this.fields = outputFileRule.getJSONArray("fields");
		this.dataSetName = outputFileRule.getString("dataSetName");
	}
	
	public String getDelimeter() {
		return delimeter;
	}
	
	public String getRowSeperator() {
		return rowSeparator;
	}
	
	public String getEncoding() {
		return encoding;
	}
	
	public boolean isIncludeHeader() {
		return includeHeader;
	}
	
	public JSONArray getFields() {
		return fields;
	}
	
	public String getDatasetName() {
		return dataSetName;
	}

	public Stream<String> generateHeaderStream() {
		JSONArray fieldsArray = getFields();
		return IntStream.range(0, fieldsArray.length()).mapToObj(index -> fieldsArray.getJSONObject(index).getString("name"));
	}
	
	public String getValue(Column column) {
		return Objects.nonNull(column) ? isTransformationRequired ? column.getTargetValue() : column.getSourceValue() : null;
	}
	
	public Stream<String> generateColumnStream(Row row) {
		JSONArray fieldsArray = getFields();
		return IntStream.range(0, fieldsArray.length()).mapToObj(i -> {
			JSONObject fieldJson = fieldsArray.getJSONObject(i);
			UUID fieldUUID = UUID.fromString(fieldJson.getString("field"));
			var column = row.getColumns().get(fieldUUID);
			return getValue(column);
		});
	}
}
