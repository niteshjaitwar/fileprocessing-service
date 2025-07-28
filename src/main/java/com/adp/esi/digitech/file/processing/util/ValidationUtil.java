package com.adp.esi.digitech.file.processing.util;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ValidationUtil {
	
	private static final String EQUALS = "equals"; 
	private static final String DATASET_ID = "dataSetId"; 
	private static final String CONFIGURATIONS = "configurations";
	
	public static boolean isHavingValue(String value) {
		return (Objects.nonNull(value) && !value.isBlank() && !value.isEmpty());
	}
	
	public static boolean isValidJson(String value) {
		if(ValidationUtil.isHavingValue(value)) {
			try {
				new JSONObject(value);
				return true;
			} catch (JSONException e) {
				log.error("ValidationUtil - isValidJson, Json Parsing failed message = {}", e.getMessage());
				return false;
			}
		}
		
		return false;
	}
	
	public static boolean isValidJsonArray(String value) {
		if(ValidationUtil.isHavingValue(value)) {
			try {
				new JSONArray(value);
				return true;
			} catch (JSONException e) {
				log.error("ValidationUtil - isValidJsonArray, Json Parsing failed message = {}", e.getMessage());
				return false;
			}
		}
		
		return false;
	}
	
	public static boolean isSkipValidaitons(String data, JSONObject dataExclusionRulesJson) {
		if(!dataExclusionRulesJson.has(ValidationUtil.EQUALS))
			return false;
		
		if(dataExclusionRulesJson.get(ValidationUtil.EQUALS) instanceof JSONArray) {
			JSONArray dataExclusionRulesJsonArray = dataExclusionRulesJson.getJSONArray(ValidationUtil.EQUALS);
			List<Object> dataExclusionRules = dataExclusionRulesJsonArray.toList();
			if(dataExclusionRules != null && !dataExclusionRules.isEmpty()) {
				List<String> list = dataExclusionRules.parallelStream().map(Objects::toString).collect(Collectors.toList());
				return list.contains(data);
			}
		}
		
		String value = dataExclusionRulesJson.getString(ValidationUtil.EQUALS);
		return data.equals(value);
	}
	
	// This Method Handles DATA_TRANSFORMATION_RULES, DATA_EXCLUSION_RULES and REMOVE_SPECIAL_CHAR
	public static JSONObject getDatasetRules(String rules, UUID globalDataSetUuid) {
		if(globalDataSetUuid == null || !ValidationUtil.isHavingValue(rules))
			return null;
		
		JSONArray dataSetArrayRules = new JSONArray(rules);
		
		OptionalInt selectedOptionalIndex  = IntStream.range(0, dataSetArrayRules.length()).parallel().filter(index -> {
			
			JSONObject dataSetRule = dataSetArrayRules.getJSONObject(index);
			if(dataSetRule != null && dataSetRule.has(ValidationUtil.DATASET_ID) && !dataSetRule.isNull(ValidationUtil.DATASET_ID)) {
				UUID currentDataSetUuid = UUID.fromString(dataSetRule.getString(ValidationUtil.DATASET_ID));									
				return globalDataSetUuid.equals(currentDataSetUuid);
			}								
			return false;
		}).findFirst();
		
		if(selectedOptionalIndex.isEmpty())
			return null;
		
		int selectedIndex = selectedOptionalIndex.getAsInt();
		var selectedDataSetRules = dataSetArrayRules.getJSONObject(selectedIndex);
		
		if(!selectedDataSetRules.has(ValidationUtil.CONFIGURATIONS) || selectedDataSetRules.isNull(ValidationUtil.CONFIGURATIONS))
			return null;
		
		return selectedDataSetRules.getJSONObject(ValidationUtil.CONFIGURATIONS);
	}
}
