package com.adp.esi.digitech.file.processing.exception;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.ErrorResponse;
import com.adp.esi.digitech.file.processing.model.FPSRequest;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.notification.model.EmailNotificationData;
import com.adp.esi.digitech.file.processing.notification.service.EmailNotificationService;
import com.adp.esi.digitech.file.processing.request.service.FPSRequestService;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractFileprocessingExpectionHandler {
	
	
	EmailNotificationService emailNotificationService;
	
	
	FPSRequestService fpsRequestService;

	@Autowired
	public void setEmailNotificationService(EmailNotificationService emailNotificationService) {
		this.emailNotificationService = emailNotificationService;
	}

	@Autowired
	public void setDataProcessingRequestService(FPSRequestService fpsRequestService) {
		this.fpsRequestService = fpsRequestService;
	}
	
	public void updateRequest(RequestContext requestContext, String errorType, String errorDetails, List<ErrorData> errors) {
		FPSRequest dataProcessingRequest = new FPSRequest();
		dataProcessingRequest.setUniqueId(requestContext.getUniqueId());
		dataProcessingRequest.setUuid(requestContext.getRequestUuid());
		dataProcessingRequest.setBu(requestContext.getBu());
		dataProcessingRequest.setPlatform(requestContext.getPlatform());
		dataProcessingRequest.setDataCategory(requestContext.getDataCategory());
		dataProcessingRequest.setStatus("Failed");
		dataProcessingRequest.setErrorType(errorType);
		
		errorDetails = ValidationUtil.isHavingValue(errorDetails) ? errorDetails : "";
		
		if(errors != null)
		try {
			String errorJson = new ObjectMapper().writeValueAsString(errors);
			errorDetails = errorDetails.concat(" - errors").concat(errorJson);
		} catch (JsonProcessingException e) {
			log.error("AutomationhubFileprocessingDefaultExpectionHandler - updateRequest(), uniqueId = {},  error = {}", requestContext.getUniqueId(), e.getMessage());
		}
		dataProcessingRequest.setErrorDetails(errorDetails);
		fpsRequestService.update(dataProcessingRequest);
	}
	
	public void sendExceptionEmail(RequestContext requestContext, String message, Throwable cause, List<ErrorData> errors) {
		EmailNotificationData emailNotificationData = new EmailNotificationData();
		emailNotificationData.setRequestContext(requestContext);
		emailNotificationData.setRootError(message);
		if(Objects.nonNull(errors)) {
			emailNotificationData.setErrors(errors);
		}
		if(Objects.nonNull(cause)) {
			emailNotificationData.setRootCasue(cause.getMessage());
		}
		try {
			emailNotificationService.sendExceptionEmail("", emailNotificationData);
		} catch (IOException ioException) {
			log.error("AutomationhubFileprocessingDefaultExpectionHandler - sendExceptionEmail(), uniqueId = {},  error = {}", requestContext.getUniqueId(), ioException.getMessage());
		}
	}
	
	public ApiResponse<String> apiResponse(HttpStatus status, String message, List<ErrorData> errors) {
		ErrorResponse error = new ErrorResponse(status.toString(), message, errors);
		return ApiResponse.error(Status.ERROR, error);
	}

}
