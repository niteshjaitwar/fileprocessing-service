package com.adp.esi.digitech.file.processing.exception;

import com.adp.esi.digitech.file.processing.model.RequestContext;

public class TransformationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private RequestContext requestContext;
	
	public TransformationException(String message) {
		super(message);
	}

	public TransformationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public RequestContext getRequestContext() {
		return requestContext;
	}

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	

}
