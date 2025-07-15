package com.adp.esi.digitech.file.processing.model;

import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class SharedFile {
	
	private String name;
	
	private String appCode;
	
	@JsonIgnore
	private String path;
	
	@JsonIgnore
	private byte[] bytes;
	
	private TargetLocation location;
}
