package com.adp.esi.digitech.file.processing.generator.document.enums;

public enum BreakType {

Page("Page"),Space("Space");
	
	BreakType(String breakType) {
		this.breakType = breakType;
	}
	
	private String breakType;

	public String getBreakType() {
		return this.breakType;
	}
}
