package com.adp.esi.digitech.file.processing.enums;

public enum ReadType {
	
	HEADER("HEADER"),POSITION("POSITION"), DELIMETER("DELIMETER");
	
	ReadType(String readType) {
		this.readType = readType;
	}
	
	
	private String readType;
	
	public String getReadType() {
		return this.readType;
	}

}
