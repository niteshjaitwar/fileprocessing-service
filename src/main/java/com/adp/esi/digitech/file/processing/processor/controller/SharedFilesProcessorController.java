package com.adp.esi.digitech.file.processing.processor.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.processor.service.QueuePublisherProcessorService;
import com.adp.esi.digitech.file.processing.processor.service.SharedFilesProcessorService;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping(value = "/ahub/fileprocessing/v2/shared-files")
@Slf4j
@CrossOrigin(origins = "${app.allowed-origins}")
public class SharedFilesProcessorController extends AbstractProcessorController {
	
	
	@PostMapping("/process")
	public ResponseEntity<ApiResponse<ProcessResponse>> process(@Valid @RequestBody(required = true) RequestPayload request) throws IOException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException {
		log.info("SharedFilesProcessorController -> process() Received JSON request for file processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
		
		super.validate(request);
		var requestContext = initRequestContext(request);
		
		log.info("SharedFilesProcessorController -> process() Initialization of request context completed, uniqueId = {}, requestContext = {}", request.getUniqueId(), requestContext.getRequestUuid());
		var processResponse = customProcessorDynamicAutowireService.process(SharedFilesProcessorService.class, request, requestContext, ProcessType.async);
		
		log.info("SharedFilesProcessorController -> process() completed, uniqueId = {}, message = {}", request.getUniqueId(), "Data  processed successfully. Passed all validation rules.");
		ApiResponse<ProcessResponse> response = ApiResponse.success(Status.SUCCESS, "Data  processed successfully. Passed all validation rules.", processResponse);
		return ResponseEntity.ok().body(response);

	}
	
	@PostMapping("/process/async")
	public ResponseEntity<ApiResponse<String>> processAsync(@Valid @RequestBody(required = true) RequestPayload request) throws IOException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException {
		ApiResponse<String> response = null;
		
			log.info("SharedFilesProcessorController -> processAsync() Received JSON request for file processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
			
			super.validate(request);
			
			var requestContext = initRequestContext(request);
			
			log.info("SharedFilesProcessorController -> processAsync() Initialization of request context completed, uniqueId = {}, requestContext = {}", request.getUniqueId(), requestContext.getRequestUuid());
			customProcessorDynamicAutowireService.processAsync(QueuePublisherProcessorService.class, request, requestContext);
			
			log.info("SharedFilesProcessorController -> processAsync() completed, uniqueId = {}, message = {}", request.getUniqueId(), "Request Submitted successfully. Please check job status.");
			response = ApiResponse.success(Status.SUCCESS, "Request Submitted successfully. Please check job status.");
			return ResponseEntity.ok().body(response);
		
		/*
		catch (Exception e) {
			ErrorResponse error = new ErrorResponse("500", e.getMessage());
			response = ApiResponse.error(Status.FAILED, error);
			log.info("SharedFilesProcessorController -> processAsync() Failed, uniqueId = {}, message = {}", request.getUniqueId(), "Request Submission Failed. Please retry again.");
			return ResponseEntity.internalServerError().body(response);	
		}
		*/
	}

}