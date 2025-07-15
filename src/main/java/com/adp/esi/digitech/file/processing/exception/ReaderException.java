package com.adp.esi.digitech.file.processing.exception;

import com.adp.esi.digitech.file.processing.model.RequestContext;

public class ReaderException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4492927227454675793L;
	
	private RequestContext requestContext;

	public ReaderException(String message) {
		super(message);
	}
	
	public ReaderException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public ReaderException(String message, Throwable cause, RequestContext requestContext) {
		super(message, cause);
		this.requestContext = requestContext;
	}

	public RequestContext getRequestContext() {
		return requestContext;
	}

	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	
}