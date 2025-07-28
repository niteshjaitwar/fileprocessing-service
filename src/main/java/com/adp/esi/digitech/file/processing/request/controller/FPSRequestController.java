package com.adp.esi.digitech.file.processing.request.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.ErrorResponse;
import com.adp.esi.digitech.file.processing.model.FPSRequest;
import com.adp.esi.digitech.file.processing.request.service.FPSRequestService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/ahub/fileprocessing/v2/request/")
@CrossOrigin(origins = "${app.allowed-origins}")
@Slf4j
public class FPSRequestController {
	
	@Autowired
	FPSRequestService fpsRequestService;

	@PostMapping
	public ResponseEntity<ApiResponse<FPSRequest>> adddataProcessReq(
			@RequestBody FPSRequest fpsRequest) {

		ApiResponse<FPSRequest> response = null;

		log.info("FPSRequestController - adddataProcessReq()  Started Adding Data,  fpsRequest = {}", fpsRequest);
		try {
			var data = fpsRequestService.add(fpsRequest);
			response = ApiResponse.success(Status.SUCCESS, data);
			log.info("FPSRequestController - adddataProcessReq()  Completed Adding Data,  fpsRequest = {}", data);
		} catch (Exception e) {
			log.error("FPSRequestController - adddataProcessReq()  Failed to save Data, fpsRequest = {}, Exception Message : {} ", fpsRequest, e.getMessage());
			ErrorResponse error = new ErrorResponse("409", e.getMessage());
			response = ApiResponse.error(Status.ERROR, error);
		}

		return ResponseEntity.ok().body(response);
	}

	@PutMapping
	public ResponseEntity<ApiResponse<FPSRequest>> updatedataProcessReq(@RequestBody FPSRequest fpsRequest) {
		ApiResponse<FPSRequest> response = null;
		log.info("FPSRequestController - updatedataProcessReq()  Started Updating Data,  fpsRequest = {}", fpsRequest);
		try {
			var data = fpsRequestService.update(fpsRequest);
			response = ApiResponse.success(Status.SUCCESS, data);
			log.info("FPSRequestController - updatedataProcessReq()  Completed Updating Data,  fpsRequest = {}", data);
		} catch (Exception e) {
			log.error("FPSRequestController - updatedataProcessReq()  Failed to update Data, fpsRequest = {}, Exception Message : {} ", fpsRequest, e.getMessage());
			ErrorResponse error = new ErrorResponse("409", e.getMessage());
			response = ApiResponse.error(Status.ERROR, error);
		}

		return ResponseEntity.ok().body(response);
	}
	
	@PostMapping("/by")
	public ResponseEntity<ApiResponse<List<FPSRequest>>> getBy(
													@RequestParam(required = true, name = "bu") String bu, 
													@RequestParam(required = true, name = "platform") String platform,
													@RequestParam(required = true, name = "dataCategory") String dataCategory, 
													@RequestParam(required = true, name = "useremail") String useremail,
													@RequestParam(required = true, name = "source") String source,
													@RequestParam(required = true, name = "isAdmin") boolean isAdmin) {	
		
		ApiResponse<List<FPSRequest>> response = null;
		FPSRequest fpsRequest = new FPSRequest();
		fpsRequest.setBu(bu);
		fpsRequest.setPlatform(platform);
		fpsRequest.setDataCategory(dataCategory);
		fpsRequest.setCreatedBy(useremail);
		fpsRequest.setSourceType(source);
		
		log.info("FPSRequestController - getdataProcessReq()  Started Getting Data,  fpsRequest = {}", fpsRequest);
		try {
			var data = fpsRequestService.findBy(fpsRequest, isAdmin);
			response = ApiResponse.success(Status.SUCCESS, data);
			log.info("FPSRequestController - getdataProcessReq()  Completed Getting Data,  fpsRequest = {}", fpsRequest);	
		} catch (Exception e) {
			log.error("FPSRequestController - getdataProcessReq()  Failed to get Data, fpsRequest = {}, Exception Message : {} ", fpsRequest, e.getMessage());
			ErrorResponse error = new ErrorResponse("409", e.getMessage());
			response = ApiResponse.error(Status.ERROR, error);
		}

		return ResponseEntity.ok().body(response);
	}
	
	@GetMapping("/{id}")
	public ResponseEntity<ApiResponse<FPSRequest>> getById(@PathVariable("id") long id) {	
		
		ApiResponse<FPSRequest> response = null;	
		
		log.info("FPSRequestController - getById()  Started Getting Data,  id = {}", id);
		try {
			var data = fpsRequestService.findBy(id);
			response = ApiResponse.success(Status.SUCCESS, data);
			log.info("FPSRequestController - getById()  Completed Getting Data,  id = {}", id);	
		} catch (Exception e) {
			log.error("FPSRequestController - getById()  Failed to get Data, id = {}, Exception Message : {} ", id, e.getMessage());
			ErrorResponse error = new ErrorResponse("204", e.getMessage());
			response = ApiResponse.error(Status.ERROR, error);
		}

		return ResponseEntity.ok().body(response);
	}
	
	@PostMapping("/reprocess")
	public ResponseEntity<ApiResponse<String>> reprocessRequest(
													@RequestParam(required = true, name = "uniqueID") String uniqueID, 
													@RequestParam(required = true, name = "requestID") Long requestID,
													@RequestParam(required = true, name = "uuid") String uuid,
													@RequestParam(required = true, name = "retryStep") String retryStep 
													) {	
		
		ApiResponse<String> response = null;
		try {
			fpsRequestService.reprocessRequest(uniqueID, uuid, requestID);
			response = ApiResponse.success(Status.SUCCESS, "Reprocess Request Initiated Successfully", null);
			log.info("FPSRequestController - reprocessRequest()  Completed reprocessing request,  uniqueID = {}, requestID = {}, retryStep = {}", uniqueID,requestID,retryStep);	
		} catch (Exception e) {
			log.error("FPSRequestController - reprocessRequest()  Failed to reprocess request,  uniqueID = {}, requestID = {}, retryStep = {}", uniqueID,requestID,retryStep);
			ErrorResponse error = new ErrorResponse("409", e.getMessage());
			response = ApiResponse.error(Status.ERROR, error);
		}

		return ResponseEntity.ok().body(response);
	}

}
