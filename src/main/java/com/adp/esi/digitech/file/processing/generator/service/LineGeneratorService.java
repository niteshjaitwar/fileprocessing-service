package com.adp.esi.digitech.file.processing.generator.service;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.BooleanUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

@Service("lineGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LineGeneratorService extends AbstractLineGeneratorService{
	
	public String generate(JSONObject colObj, Row row) throws GenerationException {
		if(isSkipLine(colObj, row))	return null;
		
		if(colObj.has("col") && !colObj.isNull("col") && !colObj.getJSONArray("col").isEmpty()) {
			JSONArray colJSONArray = colObj.getJSONArray("col");
			
			return IntStream.range(0, colJSONArray.length()).mapToObj(index -> {				
				StringBuilder sb = new StringBuilder();
				var colJsonObj = colJSONArray.getJSONObject(index);
				var prefix = colJsonObj.optString("prefix","");
				var suffix = colJsonObj.optString("suffix","");
				
				var isSkipEmptyValue = false;
				if(colJsonObj.has("conditional")  && !colJsonObj.isNull("conditional")) {
					var colConditionalJsonObj = colJsonObj.getJSONObject("conditional");
					isSkipEmptyValue = colConditionalJsonObj.has("isSkipEmptyValue") && !colConditionalJsonObj.isNull("isSkipEmptyValue") 
							? BooleanUtils.toBoolean(colConditionalJsonObj.getString("isSkipEmptyValue")) : false;
				}
				
				var value = getColumnValue(colJsonObj, row) ;
				return ValidationUtil.isHavingValue(value) ? sb.append(prefix).append(value).append(suffix).toString() : isSkipEmptyValue ? "" : sb.append(prefix).append(suffix).toString();				
			}).collect(Collectors.joining());
		}
		return null;
	}
}
