package com.adp.esi.digitech.file.processing.validation.service;

import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.RequestContext;

public interface IValidationService<T> {
	
	public void validate(T data) throws ValidationException;
	
	public void setRequestContext(RequestContext requestContext);

}
