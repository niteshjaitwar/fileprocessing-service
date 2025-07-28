package com.adp.esi.digitech.file.processing.model;

import java.util.List;

import lombok.Data;

@Data
public class ProcessResponse {
	
	private String bu; 
	
	private String platform; 
	
	private String dataCategory; 
	
	private String uniqueId;
	
	private String requestUuid;
	
	private String saveFileLocation;
	
	private List<SharedFile> files;

}
