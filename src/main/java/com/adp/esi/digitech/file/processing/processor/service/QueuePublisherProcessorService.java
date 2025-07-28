package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.request.service.FPSRequestQueueService;

import lombok.extern.slf4j.Slf4j;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class QueuePublisherProcessorService extends AbstractProcessorService<Void> {
	
	IFileService iFilesService;
	
	@Autowired
	FPSRequestQueueService requestQueueService;
	
	@Override
	public Void process(RequestPayload request) throws IOException, ReaderException, ConfigurationException,
			ValidationException, TransformationException, GenerationException {
		log.info("QueuePublisherProcessorService -> process() Received JSON request for processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
		//Check for configurations are there or not present in db
		try {
			this.objectUtilsService.dataStudioConfigurationService.validateRequest(request.getBu(), request.getPlatform(), request.getDataCategory());
		} catch (ConfigurationException e) {
			e.setRequestContext(requestContext);
			throw e;
		}
		this.iFilesService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);		
		
		//Checking for files in location
		this.validate(request);
		
		this.initRequet(request);
		
		requestQueueService.save(request, requestContext.getRequestUuid());
		
		return null;
	}
	
	@Override
	public void validate(RequestPayload request) throws ValidationException {
		log.info("QueuePublisherProcessorService - process()  Started checking files from the given location. uniqueId = {}, appCode = {}", requestContext.getUniqueId(), appCode);
		// boolean isFound = request.getDocuments().parallelStream().allMatch(document -> sharedFilesService.isFileExists(document.getLocation(), appCode));
		if (request.getDocuments() == null || request.getDocuments().isEmpty()) {
			List<ErrorData> errors = new ArrayList<>();
			errors.add(new ErrorData("documents", "Documents can't be null or empty"));
			var validationException = new ValidationException("Request Validation - Documents can't be null or empty");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
		var errors = request.getDocuments().parallelStream().map(document -> {
			if (!iFilesService.isFileExists(document.getLocation(), appCode))
				return new ErrorData(document.getSourceKey(), document.getLocation());
			return null;
		}).filter(Objects::nonNull).collect(Collectors.toList());

		if (errors != null && !errors.isEmpty()) {
			var validationException = new ValidationException(
					"Request Validation - Files not present in Shared Location");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
	}

	@Override
	public Map<String, List<DataMap>> read(RequestPayload data) throws ReaderException {
		return null;
	}

	public void setiFilesService(IFileService iFilesService) {
		this.iFilesService = iFilesService;
	}
	
	/*
	 * @Autowired
	 * KafkaProducerMessagingService kafkaProducerMessagingService;
	 * 
	 * var kafkaPayload = new KafkaPayload();
	 * kafkaPayload.setPayload(request);
	 * kafkaPayload.setRequestUuid(requestContext.getRequestUuid());		
	 * kafkaProducerMessagingService.publish(kafkaPayload); 
	 */

}
