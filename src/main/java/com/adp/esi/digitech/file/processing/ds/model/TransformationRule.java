package com.adp.esi.digitech.file.processing.ds.model;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern.Flag;

import lombok.Data;

@Data
public class TransformationRule {
	
	private Long id;
	
	@NotEmpty(message = "The bu is Required")
	private String bu;
	
	@NotEmpty(message = "The platform is Required")
	private String platform;
	
	@NotEmpty(message = "The dataCategory is Required")
	private String dataCategory;
	
	private String subDataCategory;
		
	//@NotEmpty(message = "The sourceColumnName is Required")
	private String sourceColumnName;
	
	@NotEmpty(message = "The targetColumnName is Required")
	private String targetColumnName;
	
	private String targetFileName;	
	
	private Integer columnSequence;
	
	private String defaultValue;
	
	private String dataTransformationRules;
	
	private String specialCharToBeRemoved;
	
	private String transformationRequired;
	
	private String lovValidationRequired;
	
	@NotEmpty(message = "The useremail is Required")
	@Email(message = "The useremail is invalid", flags = {Flag.CASE_INSENSITIVE})
	private String useremail;
	
	@NotEmpty(message = "The userrole is Required")
	private String userrole;

}
