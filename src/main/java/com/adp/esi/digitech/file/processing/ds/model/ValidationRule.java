package com.adp.esi.digitech.file.processing.ds.model;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern.Flag;

import lombok.Data;

@Data
public class ValidationRule {
	
	private Long id;
		
	@NotEmpty(message = "The bu is Required")
	private String bu;
	
	@NotEmpty(message = "The platform is Required")
	private String platform;
	
	@NotEmpty(message = "The dataCategory is Required")
	private String dataCategory;	
	
	private String subDataCategory;	
	
	@NotEmpty(message = "The sourceColumn is Required")
	private String sourceColumn;
	
	private String sourceColumnName;
	
	@NotEmpty(message = "The dataType is Required")
	private String dataType;
	
	private String isMandatory;
	
	private String maxLengthAllowed;
	
	private String dataFormat;
	
	private String minValue;
	
	private String maxValue;
	
	private String specialCharNotAllowed;
	
	private String lovCheckType;
	
	private String transformationRequired;
	
	private String columnRequiredInErrorFile;
	
	private String uniqueValueInColumn;
	
	private String conditionalValidationRule;
	
	private String dataTransformationRules;
	
	private String specialCharToBeRemoved;
	
	private String dataExclusionRules;
	
	private String validationRuleType;
	
	private String minLengthAllowed;
	
	private String stringCheckRule;
		
	@NotEmpty(message = "The useremail is Required")
	@Email(message = "The useremail is invalid", flags = {Flag.CASE_INSENSITIVE})
	private String useremail;
	
	@NotEmpty(message = "The userrole is Required")
	private String userrole;
	
	private boolean isSkipValidaitons;
	
	private String lovValidationRequired;

}
