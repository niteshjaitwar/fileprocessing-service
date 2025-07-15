package com.adp.esi.digitech.file.processing.generator.document.enums;

public enum PageLayoutSize {

	A3("A3"),A4("A4"), A5("A5"), A6("A6");

	private String pageLayoutSize;
	
	PageLayoutSize(String pageLayoutSize) {
		this.pageLayoutSize=pageLayoutSize;
	}
	
	public String getElementType() {
		return this.pageLayoutSize;
	}
	
}
