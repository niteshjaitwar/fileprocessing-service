package com.adp.esi.digitech.file.processing.generator.service;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

@Service("positionLineGeneratorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PositionLineGeneratorService extends AbstractLineGeneratorService{
	
	public String generate(JSONObject colObj, Row row) throws GenerationException {
		if (isSkipLine(colObj, row))
			return null;

		if (colObj.has("col") && !colObj.isNull("col") && !colObj.getJSONArray("col").isEmpty()) {
			JSONArray colJSONArray = colObj.getJSONArray("col");
		/*	
			var isSkipEmptyValue = false; //is it required
			if(colJsonObj.has("conditional")  && !colJsonObj.isNull("conditional")) {
				var colConditionalJsonObj = colJsonObj.getJSONObject("conditional");
				isSkipEmptyValue = colConditionalJsonObj.has("isSkipEmptyValue") && !colConditionalJsonObj.isNull("isSkipEmptyValue") 
						? BooleanUtils.toBoolean(colConditionalJsonObj.getString("isSkipEmptyValue")) : false;
			}
		*/	
			return IntStream.range(0, colJSONArray.length()).mapToObj(index -> {
				StringBuilder sb = new StringBuilder();
				var colJsonObj = colJSONArray.getJSONObject(index);
				var position = colJsonObj.optInt("position", 0);

				var value = getColumnValue(colJsonObj, row);
				return getPositionValue(value, position, sb);
			}).collect(Collectors.joining());
		}
		return null;
	}
	
	private String getPositionValue(String value, int position, StringBuilder sb) {
		if (ValidationUtil.isHavingValue(value)) {
			int lengthDiff = position - sb.length();
			if (lengthDiff > 0)
				sb.append(" ".repeat(lengthDiff));
			else if (lengthDiff < 0)
				sb.delete(position, sb.length());
			sb.insert(position, value);
		}
		return sb.toString();
	}
}
