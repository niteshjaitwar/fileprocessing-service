package com.adp.esi.digitech.file.processing.enums;

public enum TargetLocation {

	SharePoint("SharePoint"), SharedDrive("SharedDrive"), Local("Local");

	TargetLocation(String targetLocation) {
		this.targetLocation = targetLocation;
	}

	private String targetLocation;

	public String getTargetLocation() {
		return this.targetLocation;
	}
}
