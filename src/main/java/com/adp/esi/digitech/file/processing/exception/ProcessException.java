package com.adp.esi.digitech.file.processing.exception;

import com.adp.esi.digitech.file.processing.model.RequestContext;

public class ProcessException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5179470246573317614L;	
	
	private RequestContext requestContext;

	public ProcessException(String message) {
		super(message);
	}
	
	public ProcessException(String message, Throwable cause) {
		super(message, cause);
	}

	public RequestContext getRequestContext() {
		return requestContext;
	}

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

}
