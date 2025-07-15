package com.adp.esi.digitech.file.processing.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Status {
	SUCCESS("success"), ERROR("error"), FAILED("failed"),
	CLIENT_DATA_VALIDATION("CLIENT_DATA_VALIDATION"), 
	CAM_DATA_VALIDATION("CAM_DATA_VALIDATION"),
	DATA_TRANSFORMATION("DATA_TRANSFORMATION"),
	PENDING("PENDING"), PROCESSING("PROCESSING"), COMPLETED("COMPLETED"), FAILED_PROCESSING("FAILED_PROCESSING"),;
	
	private final String status;
	
	Status(String status) {
		this.status = status;
	}
	
	@JsonValue
	public String getStatus() {
		return status;
	}
}
