package com.adp.esi.digitech.file.processing.validation.service;

import com.adp.esi.digitech.file.processing.model.RequestContext;


public abstract class AbstractValidationService<T> implements IValidationService<T> {
	
	RequestContext requestContext;	
	
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
}
