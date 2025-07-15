package com.adp.esi.digitech.file.processing.generator.util;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.autowire.service.CustomGeneratorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.generator.service.AbstractLineGeneratorService;
import com.adp.esi.digitech.file.processing.model.RequestContext;

@Service
public class TextGeneratorUtils {
	
	@Autowired
	CustomGeneratorDynamicAutowireService customGeneratorDynamicAutowireService;
	
	public boolean isRowRepeat(JSONObject outputFileRule) {
		if (outputFileRule.has("repeat") && !outputFileRule.isNull("repeat"))
			return outputFileRule.getBoolean("repeat");
		
		return false;
	}

	public <T extends AbstractLineGeneratorService> StringBuilder constructHeaders(Class<T> type, JSONObject outputFileRule, StringBuilder sb, RequestContext requestContext) {
		if (outputFileRule.has("headers") && !outputFileRule.isNull("headers")) {
			var headersJsonArray = outputFileRule.getJSONArray("headers");
			if(headersJsonArray.length() > 0) {
				var headerLines = IntStream.range(0, headersJsonArray.length())
						.mapToObj(index -> customGeneratorDynamicAutowireService.generate(type, headersJsonArray.getJSONObject(index), null, requestContext))
						.filter(Objects::nonNull).collect(Collectors.joining("\n"));
				sb.append(headerLines);
				sb.append("\n");
			}
		}
		return sb;
	}

}
