package com.adp.esi.digitech.file.processing.exception;

import java.util.List;

import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.RequestContext;

public class ConfigurationException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6738289860296647495L;
	
	private List<ErrorData> errors;
	
	private RequestContext requestContext;

	public ConfigurationException(String message) {
		super(message);
		
	}
	
	public ConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	
	
	public RequestContext getRequestContext() {
		return requestContext;
	}

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	public void setErrors(List<ErrorData> errors) {
		this.errors = errors;
	}

	public List<ErrorData> getErrors() {
		return errors;
	}

}
