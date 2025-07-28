package com.adp.esi.digitech.file.processing.enums;

public enum ValidationType {
	
	client("client"), CAM("CAM");
	
	ValidationType(String validationType) {
		this.validationType = validationType;
	}
	
	
	private String validationType;
	
	public String getValidationType() {
		return this.validationType;
	}
}
