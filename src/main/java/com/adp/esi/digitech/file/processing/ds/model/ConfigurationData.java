package com.adp.esi.digitech.file.processing.ds.model;

import jakarta.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class ConfigurationData {

	private Long id;
	
	@NotEmpty(message = "The bu is Required")
	private String bu;
		
	@NotEmpty(message = "The platform is Required")
	private String platform;
	
	@NotEmpty(message = "The dataCategory is Required")
	private String dataCategory;
	
	private String subDataCategory;	
	
	private String outputFileRules;
	
	private String appCode;	
	
	private String source;	
	
	private String inputRules;
	
	@JsonProperty("useremail")
	@NotEmpty(message = "The useremail is Required")
	private String useremail;
	
	@JsonProperty("userrole")
	@NotEmpty(message = "The userrole is Required")
	private String userrole;
	
	private String filesInfo;
	
	private String dataRules;
	
	private String targetLocation;
	
	private String targetPath;
	
	private String processType;
	
	private String processSteps;
}
