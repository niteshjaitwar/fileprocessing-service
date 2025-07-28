package com.adp.esi.digitech.file.processing.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RequestStatus {
	Submitted("Submitted"),ReSubmitted("ReSubmitted"), Started("Started"), Read("Read"), 
	Validate("Validate"), Transform("Transform"), File("File"), Completed("Completed"),
	Configuration_Error("Configuration Error"), Validation_Error("Validation Error"),
	Transformation_Error("Transformation Error"), Process_Error("Process Error");
	//[Submitted,Started,Read,Validate,Transform,File,Completed]
	//[Configuration Error,Validation Error, Transformation Error,Process Error]
	
	private final String status;
	
	RequestStatus(String status) {
		this.status = status;
	}
	
	@JsonValue
	public String getRequestStatus() {
		return status;
	}
}
