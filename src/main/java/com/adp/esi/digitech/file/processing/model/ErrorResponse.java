package com.adp.esi.digitech.file.processing.model;

import java.util.List;

public final class ErrorResponse {
	
	public final String code;
	public final String message;
	public List<ErrorData> errors;
	
	
	public ErrorResponse(String code, String message) {
		this.code = code;
		this.message = message;
	}
	
	
	
	public ErrorResponse(String code, String message, List<ErrorData> errors) {
		this.code = code;
		this.message = message;
		this.errors = errors;
	}

}
