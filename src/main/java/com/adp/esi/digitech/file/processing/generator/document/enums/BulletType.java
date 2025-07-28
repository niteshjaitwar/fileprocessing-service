package com.adp.esi.digitech.file.processing.generator.document.enums;

public enum BulletType {

	Number("Numbers"), Alphabet_Lower_Case("Alphabet Lower Case"),Alphabet_Upper_Case("Alphabet Upper Case"),Unicode("Unicode"), Symbol("Symbol"), None("None");
	
	private String bulletType;
	
	BulletType(String bulletType) {
		this.bulletType=bulletType;
	}
	
	public String getBulletType() {
		return this.bulletType;
	}
}
