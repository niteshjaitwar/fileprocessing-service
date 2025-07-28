package com.adp.esi.digitech.file.processing.model;

import jakarta.validation.constraints.NotEmpty;

import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class Document {
	
	@NotEmpty(message = "sourceKey is Required")
	private String sourceKey;
	
	@NotEmpty(message = "location is Required")
	private String location;
	
	@JsonIgnore
	private MultipartFile file;
	
	@JsonIgnore
	private String localPath;
	
	//@JsonIgnore
	//private Map<String,List<ColumnRelation>> columnRelationsMap;
}
