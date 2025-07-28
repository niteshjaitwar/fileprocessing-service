package com.adp.esi.digitech.file.processing.generator.document.enums;

public enum TransposeType {
	Yes("Y"),No("N");

	private String transposeType;
	TransposeType(String transposeType) {
		this.transposeType=transposeType;
	}
	
	public String getTranposeType() {
		return this.transposeType;
	}
}
