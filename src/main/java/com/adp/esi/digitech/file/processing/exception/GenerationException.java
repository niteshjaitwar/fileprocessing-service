package com.adp.esi.digitech.file.processing.exception;

import com.adp.esi.digitech.file.processing.model.RequestContext;

public class GenerationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7525021972942230433L;
	
	private RequestContext requestContext;

	public GenerationException(String message) {
		super(message);
	}
	
	public GenerationException(String message, Throwable cause) {
		super(message, cause);
	}

	public RequestContext getRequestContext() {
		return requestContext;
	}

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	
}