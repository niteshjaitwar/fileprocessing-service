package com.adp.esi.digitech.file.processing.generator.document.config.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class FileName {
	private List<String> columns;
	private List<String> suffix;
	private String seperator;
	private String isTransformationRequired;
	
	@JsonProperty("static name")
	private String staticName;
}