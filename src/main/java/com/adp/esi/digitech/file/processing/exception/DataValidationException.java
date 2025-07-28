package com.adp.esi.digitech.file.processing.exception;

import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.file.processing.enums.ValidationType;
import com.adp.esi.digitech.file.processing.model.Row;

public class DataValidationException extends ValidationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6532587457348252299L;
	
	private ValidationType validationType;
	
	private Map<String,List<Row>> errorsMap;
	
	public DataValidationException(String message) {
		super(message);
	}
	
	public DataValidationException(String message, Throwable cause) {
		super(message, cause);
	}
	public DataValidationException(String message,ValidationType validationType) {
		super(message);
		this.validationType = validationType;
	}
	public Map<String,List<Row>> getErrorsMap() {
		return errorsMap;
	}

	public void setErrorsMap(Map<String, List<Row>> errorsMap) {
		this.errorsMap = errorsMap;
	}
	
	public ValidationType getValidationType() {
		return validationType;
	}

	public void setValidationType(ValidationType validationType) {
		this.validationType = validationType;
	}
	
}
