package com.adp.esi.digitech.file.processing.notification.model;

import java.util.List;

import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.RequestContext;

import lombok.Data;

@Data
public class EmailNotificationData {
	
	private RequestContext requestContext;
	
	private String rootError;
	
	private String rootCasue;
	
	private List<ErrorData> errors;
	
	private List<String> filesPath;

}
