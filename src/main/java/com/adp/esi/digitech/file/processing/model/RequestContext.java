package com.adp.esi.digitech.file.processing.model;

import lombok.Data;

@Data
public class RequestContext {
	
	private String bu; 
	
	private String platform; 
	
	private String dataCategory; 
	
	private String uniqueId;
	
	private String requestUuid;
	
	private String saveFileLocation;
	
	//private boolean debugLoggingRequired;

}
