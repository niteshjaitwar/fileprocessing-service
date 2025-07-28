package com.adp.esi.digitech.file.processing.exception;

import java.util.List;

import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.RequestContext;

public class ValidationException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1738253987372902658L;
	private List<ErrorData> errors;
	private RequestContext requestContext;

	public ValidationException(String message) {
		super(message);
	}
	public ValidationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	
	public void setErrors(List<ErrorData> errors) {
		this.errors = errors;
	}
	public List<ErrorData> getErrors() {
		return errors;
	}
	public RequestContext getRequestContext() {
		return requestContext;
	}
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	
}
