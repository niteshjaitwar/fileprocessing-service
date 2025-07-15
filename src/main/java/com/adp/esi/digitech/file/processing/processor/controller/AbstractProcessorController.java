package com.adp.esi.digitech.file.processing.processor.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.adp.esi.digitech.file.processing.autowire.service.CustomProcessorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.RequestContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AbstractProcessorController implements IProcessorController {
	
	CustomProcessorDynamicAutowireService customProcessorDynamicAutowireService;	
	
	@Autowired
	public void setCustomProcessorDynamicAutowireService(
			CustomProcessorDynamicAutowireService customProcessorDynamicAutowireService) {
		this.customProcessorDynamicAutowireService = customProcessorDynamicAutowireService;
	}

	public void validate(RequestPayload request) throws ValidationException{		
		if(request == null)
			throw new ValidationException("request cannot be null");
		
		List<ErrorData> errors = new ArrayList<>();
		log.info("AbstractProcessorController -> validate() Received JSON request for basic validations, uniqueId = {}", request.getUniqueId());
		if(!ValidationUtil.isHavingValue(request.getBu()))
			errors.add(new ErrorData("BU", "BU is Required"));
		
		if(!ValidationUtil.isHavingValue(request.getPlatform()))
			errors.add(new ErrorData("Platform", "Platform is Required"));
		
		if(!ValidationUtil.isHavingValue(request.getDataCategory()))
			errors.add(new ErrorData("DataCategory", "DataCategory is Required"));
		
		if(!ValidationUtil.isHavingValue(request.getUniqueId()))
			errors.add(new ErrorData("UniqueId", "UniqueId is Required"));
		
		if(!ValidationUtil.isHavingValue(request.getSaveFileLocation()))
			errors.add(new ErrorData("SaveFileLocation", "SaveFileLocation is Required"));
		
		if(errors != null && !errors.isEmpty()) {
			var validationException = new ValidationException("Request Validation Failed");
			validationException.setErrors(errors);
			throw validationException;
		}
		
		log.info("AbstractProcessorController -> validate() Completed JSON request for basic validations, uniqueId = {}", request.getUniqueId());
		
	}


	public RequestContext initRequestContext(RequestPayload request) {
		RequestContext requestContext = new RequestContext();
		requestContext.setBu(request.getBu());
		requestContext.setPlatform(request.getPlatform());
		requestContext.setDataCategory(request.getDataCategory());
		requestContext.setUniqueId(request.getUniqueId());
		requestContext.setRequestUuid(UUID.randomUUID().toString());
		requestContext.setSaveFileLocation(request.getSaveFileLocation());
		
		return requestContext;
	}	
	
}
