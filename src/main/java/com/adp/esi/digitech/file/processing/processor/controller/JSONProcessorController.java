package com.adp.esi.digitech.file.processing.processor.controller;

import java.io.IOException;

import org.json.JSONObject;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
import com.adp.esi.digitech.file.processing.processor.service.JSONAsyncProcessorService;
import com.adp.esi.digitech.file.processing.processor.service.JSONProcessorService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping(value = "/ahub/fileprocessing/v2/json")
@Slf4j
public class JSONProcessorController extends AbstractProcessorController {
	
	
	@PostMapping("/process")
	public ResponseEntity<ApiResponse<ProcessResponse>> process(@RequestBody(required = true) RequestPayload request) throws IOException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException {
		log.info("JSONProcessorController -> process() Received JSON request for file processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
		
		super.validate(request);
		var requestContext = initRequestContext(request);
		
		log.info("JSONProcessorController -> process() Initialization of request context completed, uniqueId = {}, requestContext = {}", request.getUniqueId(), requestContext.getRequestUuid());
		
		var processResponse = customProcessorDynamicAutowireService.process(JSONProcessorService.class, request, requestContext, ProcessType.async);
		log.info("JSONProcessorController -> process() completed, uniqueId = {}, message = {}", request.getUniqueId(), "Data  processed successfully. Passed all validation rules.");
		ApiResponse<ProcessResponse> response = ApiResponse.success(Status.SUCCESS, "Data  processed successfully. Passed all validation rules.", processResponse);
		return ResponseEntity.ok().body(response);	
	}
	
	@PostMapping("/process/async")
	public ResponseEntity<ApiResponse<String>> processAsync(@RequestBody(required = true) RequestPayload request) throws IOException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException {
		ApiResponse<String> response = null;
			log.info("JSONProcessorController -> processAsync() Received JSON request for file processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
			
			super.validate(request);
			var requestContext = initRequestContext(request);
			
			log.info("JSONProcessorController -> processAsync() Initialization of request context completed, uniqueId = {}, requestContext = {}", request.getUniqueId(), requestContext.getRequestUuid());
			
			customProcessorDynamicAutowireService.processAsync(JSONAsyncProcessorService.class,request, requestContext);
			
			log.info("JSONProcessorController -> processAsync() completed, uniqueId = {}, message = {}", request.getUniqueId(), "Request Submitted successfully. Please check job status.");
			response = ApiResponse.success(Status.SUCCESS, "Request Submitted successfully. Please check job status.");
			return ResponseEntity.ok().body(response);	
		/*
		catch (Exception e) {
			ErrorResponse error = new ErrorResponse("500", e.getMessage());
			response = ApiResponse.error(Status.FAILED, error);
			log.info("JSONProcessorController -> processAsync() Failed, uniqueId = {}, message = {}", request.getUniqueId(), "Request Submission Failed. Please retry again.");
			return ResponseEntity.internalServerError().body(response);	
		}*/
	}
	
	
	@PostMapping
	public ResponseEntity<ApiResponse<ProcessResponse>> process(@RequestParam("bu") String bu
			,@RequestParam("platform") String platform
			,@RequestParam("dataCategory") String dataCategory
			,@RequestParam("uniqueId") String uniqueId			
			,@RequestParam("saveFileLocation") String saveFileLocation
			,@RequestParam("jsonPayload")  String jsonPayload)  throws IOException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException {
		
		log.info("JSONProcessorController -> process() Received JSON request for file processing, uniqueId = {}, Bu = {}, Platform = {}, DataCategory = {}, request = {}", uniqueId, bu, platform, dataCategory, jsonPayload);
			
		JSONObject requestPayload =new JSONObject(jsonPayload); 
			
		JSONObject rawPayLoad = new JSONObject();
		rawPayLoad.put("text", requestPayload);
			
		RequestPayload request = new RequestPayload();
		request.setBu(bu);
		request.setDataCategory(dataCategory);
		request.setPlatform(platform);
		request.setUniqueId(uniqueId);
		request.setSaveFileLocation(saveFileLocation);
		request.setRawJsonPayload(rawPayLoad);
		
		super.validate(request);
		var requestContext = initRequestContext(request);
		
		log.info("JSONProcessorController -> process() Initialization of request context completed, uniqueId = {}, requestContext = {}", request.getUniqueId(), requestContext.getRequestUuid());
		
		var processResponse = customProcessorDynamicAutowireService.process(JSONProcessorService.class, request, requestContext, ProcessType.async);
		log.info("JSONProcessorController -> process() completed, uniqueId = {}, message = {}", request.getUniqueId(), "Data  processed successfully. Passed all validation rules.");
		ApiResponse<ProcessResponse> response = ApiResponse.success(Status.SUCCESS, "Data  processed successfully. Passed all validation rules.", processResponse);
		return ResponseEntity.ok().body(response);	
		
	}
}
