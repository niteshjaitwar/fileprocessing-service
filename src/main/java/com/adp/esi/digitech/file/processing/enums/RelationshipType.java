package com.adp.esi.digitech.file.processing.enums;

public enum RelationshipType {
	
	OnetoOne("OnetoOne"),AlltoOne("AlltoOne"),ManytoOne("ManytoOne"),OnetoMany("OnetoMany");
	
	RelationshipType(String relationshipType) {
		this.relationshipType = relationshipType;
	}
	
	
	private String relationshipType;
	
	public String getRelationshipType() {
		return this.relationshipType;
	}

}
