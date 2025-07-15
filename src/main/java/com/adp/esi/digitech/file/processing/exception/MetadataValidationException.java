package com.adp.esi.digitech.file.processing.exception;

public class MetadataValidationException extends ValidationException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 453863729667698578L;
	

	public MetadataValidationException(String message) {
		super(message);
	}

	public MetadataValidationException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
