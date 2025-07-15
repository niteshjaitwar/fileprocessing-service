package com.adp.esi.digitech.file.processing.processor.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.processor.service.ExcelProcessorService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping(value = "/ahub/fileprocessing/v2/excel")
@Slf4j
public class ExcelProcessorController extends AbstractProcessorController {
	
	
	@PostMapping("/upload")
	public ResponseEntity<ApiResponse<ProcessResponse>> process(@RequestParam(name = "bu",required = true) String bu
										 ,@RequestParam(name = "platform") String platform
										 ,@RequestParam(name = "dataCategory") String dataCategory
										 ,@RequestParam("uniqueId") String uniqueId
										 ,@RequestParam(name = "file", required = true) MultipartFile file
										 ,@RequestParam("saveFileLocation") String saveFileLocation
										 ) throws IOException, ReaderException, ValidationException, TransformationException, GenerationException, ProcessException {		
		
		log.info("ExcelProcessorController - process(), Received request for processing. BU = {}, Platform = {}, Data Category = {}, Unique ID = {}, Location to Save File = {}, file name = {}, file type = {}, file size = {} bytes ", bu, platform, dataCategory, uniqueId, saveFileLocation, file.getOriginalFilename(), file.getContentType(), file.getSize());
		
		RequestPayload request = new RequestPayload();
		request.setBu(bu);
		request.setDataCategory(dataCategory);
		request.setPlatform(platform);
		request.setUniqueId(uniqueId);
		request.setSaveFileLocation(saveFileLocation);
		request.setFile(file);
		
		super.validate(request);
		var requestContext = initRequestContext(request);
		
		log.info("ExcelProcessorController -> process() Initialization of request context completed, uniqueId = {}, requestContext = {}", request.getUniqueId(), requestContext.getRequestUuid());
		
		var processResponse = customProcessorDynamicAutowireService.process(ExcelProcessorService.class, request, requestContext, ProcessType.async);
		log.info("ExcelProcessorController -> process() completed, uniqueId = {}, message = {}", request.getUniqueId(), "Data  processed successfully. Passed all validation rules.");
		ApiResponse<ProcessResponse> response = ApiResponse.success(Status.SUCCESS, "Data  processed successfully. Passed all validation rules.", processResponse );
		return ResponseEntity.ok().body(response);
	}

}
