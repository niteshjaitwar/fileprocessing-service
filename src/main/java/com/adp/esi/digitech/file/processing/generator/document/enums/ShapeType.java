package com.adp.esi.digitech.file.processing.generator.document.enums;

public enum ShapeType {

	Box("Box");

	private String shapeType;
	
	ShapeType(String shapeType) {
		this.shapeType=shapeType;
	}
	
	public String getShapeType() {
		return this.shapeType;
	}
}
