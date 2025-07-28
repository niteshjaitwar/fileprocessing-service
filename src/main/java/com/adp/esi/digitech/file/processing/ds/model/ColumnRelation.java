package com.adp.esi.digitech.file.processing.ds.model;

import jakarta.validation.constraints.NotEmpty;

import lombok.Data;

@Data
public class ColumnRelation {
	
	private Long id;
	
	@NotEmpty(message = "The bu is Required")
	private String bu;
		
	@NotEmpty(message = "The platform is Required")
	private String platform;
	
	@NotEmpty(message = "The dataCategory is Required")
	private String dataCategory;
		
	@NotEmpty(message = "The sourceKey is Required")
	private String sourceKey;
	
	@NotEmpty(message = "The columnName is Required")
	private String columnName;	
	
	private Long position;
	
	private String aliasName;
	
	private String uuid;
	
	private String required;
	
	private String columnRequiredInErrorFile;
	
	private String dataExclusionRules;
	
	@NotEmpty(message = "The useremail is Required")
	private String useremail;
	
	@NotEmpty(message = "The userrole is Required")
	private String userrole;
	
	private String path;
	
	private String dataType;
	
	private String format;
	

}
