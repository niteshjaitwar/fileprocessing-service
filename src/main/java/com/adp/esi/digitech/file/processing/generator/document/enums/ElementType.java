package com.adp.esi.digitech.file.processing.generator.document.enums;

public enum ElementType {
	
	
	Paragraph("Paragraph"), Table("Table"), Image("Image"), Shape("shape"), HDList("HDList"), Break("Break");

	
	
	ElementType(String elementType) {
		this.elementType = elementType;
		
		
	}
	
	private String elementType;

	public String getElementType() {
		return this.elementType;
	}

}
