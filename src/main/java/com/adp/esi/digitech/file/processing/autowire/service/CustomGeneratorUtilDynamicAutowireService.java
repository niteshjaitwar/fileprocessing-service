package com.adp.esi.digitech.file.processing.autowire.service;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.file.processing.generator.util.CSVGeneratorUtils;

@Service
public class CustomGeneratorUtilDynamicAutowireService {

private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomGeneratorUtilDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public CSVGeneratorUtils getCSVGeneratorUtils(JSONObject outputFileRule, String isTransformationRequired) {
		return webApplicationContext.getBean(CSVGeneratorUtils.class, outputFileRule, isTransformationRequired);		
	}
}
