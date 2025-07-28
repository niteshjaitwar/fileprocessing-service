package com.adp.esi.digitech.file.processing.generator.service;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;

import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

public abstract class AbstractLineGeneratorService  {
	
	public RequestContext requestContext;
	public String isTransformationRequired;
	
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	public void setIsTransformationRequired(String isTransformationRequired) {
		this.isTransformationRequired = isTransformationRequired;
	}
	
	public Stream<Boolean> isNull(JSONArray columnsArray, Row row) {
		return IntStream.range(0, columnsArray.length()).mapToObj(index -> {
			UUID key = UUID.fromString(columnsArray.optString(index));
			var column = row.getColumns().get(key);
			if (Objects.isNull(column))
				return true;

			return !(isTransformationRequired.equalsIgnoreCase("N")
					? ValidationUtil.isHavingValue(column.getSourceValue())
					: ValidationUtil.isHavingValue(column.getTargetValue()));

		});
	}
	
	protected boolean isSkipLine(JSONObject colObj, Row row) {
		var isKipLine = colObj.optString("isSkipLine", "N");
		var operator = colObj.optString("operator");
		var columnsArray = colObj.optJSONArray("columns");
		if ("Y".equalsIgnoreCase(isKipLine)) {
			return "and".equalsIgnoreCase(operator) ? isNull(columnsArray, row).allMatch(Boolean::booleanValue)
					: isNull(columnsArray, row).anyMatch(Boolean::booleanValue);
		}
		return false;
	}
	
	protected String getColumnValue(JSONObject colJsonObj, Row row) {
		var value = colJsonObj.optString("value", "");
		if (value.startsWith("{{") && value.endsWith("}}") && row != null && row.getColumns() != null) {
			String col = value.substring(2, value.length() - 2);
			UUID key = UUID.fromString(col);

			if (row.getColumns().containsKey(key)) {
				var column = row.getColumns().get(key);
				value = isTransformationRequired.equalsIgnoreCase("N") ? column.getSourceValue()
						: column.getTargetValue();
			}
		}
		return value;
	}

	public abstract String generate(JSONObject colRule, Row data);

}
