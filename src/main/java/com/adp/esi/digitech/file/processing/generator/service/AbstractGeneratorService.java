package com.adp.esi.digitech.file.processing.generator.service;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.file.processing.autowire.service.CustomGeneratorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.autowire.service.CustomGeneratorUtilDynamicAutowireService;
import com.adp.esi.digitech.file.processing.model.RequestContext;

public abstract class AbstractGeneratorService<T,R> implements IGeneratorService<T,R> {
	
	RequestContext requestContext;	
	
	String isTransformationRequired;
	
	CustomGeneratorDynamicAutowireService customGeneratorDynamicAutowireService;
	CustomGeneratorUtilDynamicAutowireService customGeneratorUtilDynamicAutowireService;

	@Autowired
	protected void setCustomGeneratorDynamicAutowireService(
			CustomGeneratorDynamicAutowireService customGeneratorDynamicAutowireService) {
		this.customGeneratorDynamicAutowireService = customGeneratorDynamicAutowireService;
	}	
	
	@Autowired
	protected void setCustomGeneratorUtilDynamicAutowireService(
			CustomGeneratorUtilDynamicAutowireService customGeneratorUtilDynamicAutowireService) {
		this.customGeneratorUtilDynamicAutowireService = customGeneratorUtilDynamicAutowireService;
	}
	
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	public void setIsTransformationRequired(String isTransformationRequired) {
		this.isTransformationRequired = isTransformationRequired;
	}
	
	public Charset getCharset(String encoding) {
		if (Objects.isNull(encoding) || encoding.isEmpty()) {
			return Charset.defaultCharset();
		}
		
		switch (encoding.toUpperCase()) {
		case "UTF-8":
			return StandardCharsets.UTF_8;
		case "UTF-16":
			return StandardCharsets.UTF_16;
		case "ISO-8859-1":
			return StandardCharsets.ISO_8859_1;
		default:
			return Charset.defaultCharset();
		}
	}
	
}
