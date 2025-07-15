package com.adp.esi.digitech.file.processing.generator.service;

import java.util.Map;

import org.json.JSONObject;

import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.RequestContext;

public interface IGeneratorService<T,R> {
	
	public void setRequestContext(RequestContext requestContext);
	
	public void setIsTransformationRequired(String isTransformationRequired);
	
	public R generate(JSONObject outputFileRule, Map<String, DataSet<T>> data) throws GenerationException;
	
	//public void generateLarge(JSONObject outputFileRule, V data) throws GenerationException;
}
