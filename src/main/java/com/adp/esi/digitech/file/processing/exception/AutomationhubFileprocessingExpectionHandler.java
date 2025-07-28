package com.adp.esi.digitech.file.processing.exception;

import java.util.HashMap;
import java.util.Map;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.ErrorResponse;

@ControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AutomationhubFileprocessingExpectionHandler extends AbstractFileprocessingExpectionHandler {	
	
	@ResponseStatus(HttpStatus.OK)
	@ExceptionHandler({DataValidationException.class})
	public ResponseEntity<ApiResponse<String>> dataValidationException(DataValidationException e) {

		var response = apiResponse(HttpStatus.PRECONDITION_FAILED, e.getMessage(), e.getErrors());
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), e.getErrors());
		
		this.updateRequest(e.getRequestContext(), "Data Validation", e.getMessage(),e.getErrors());
				
		return ResponseEntity.ok().body(response);
	}
	
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ExceptionHandler({MetadataValidationException.class})
	public ResponseEntity<ApiResponse<String>> metaDataException(MetadataValidationException e) {
		
		var response = apiResponse(HttpStatus.BAD_REQUEST, e.getMessage(), e.getErrors());		
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), e.getErrors());
		
		this.updateRequest(e.getRequestContext(), "Metadata Validation", e.getMessage(),e.getErrors());
		
		return ResponseEntity.badRequest().body(response);
	}
	
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ExceptionHandler({ValidationException.class})
	public ResponseEntity<ApiResponse<String>> validationException(ValidationException e) {
		
		var response = apiResponse(HttpStatus.BAD_REQUEST, e.getMessage(), e.getErrors());		
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), e.getErrors());
		
		this.updateRequest(e.getRequestContext(), "Validation", e.getMessage(),e.getErrors());
		
		return ResponseEntity.badRequest().body(response);
	}
	
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ExceptionHandler({ConfigurationException.class})
	public ResponseEntity<ApiResponse<String>> configurationException(ConfigurationException e) {
		
		var response = apiResponse(HttpStatus.BAD_REQUEST, e.getMessage(), e.getErrors());
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), e.getErrors());
		
		this.updateRequest(e.getRequestContext(), "Configuration", e.getMessage(), e.getErrors());
		
		return ResponseEntity.badRequest().body(response);
	}
	
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ExceptionHandler({TransformationException.class})
	public ResponseEntity<ApiResponse<String>> transformationException(TransformationException e) {
		
		var response = apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), null);
		
		this.updateRequest(e.getRequestContext(), "Transformation", e.getMessage(), null);
		
		return ResponseEntity.internalServerError().body(response);
	}
	
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ExceptionHandler({GenerationException.class})
	public ResponseEntity<ApiResponse<String>> generationException(GenerationException e) {
		
		var response = apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);		

		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), null);
		
		this.updateRequest(e.getRequestContext(), "Generation", e.getMessage(), null);
		
		return ResponseEntity.internalServerError().body(response);
	}
	
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ExceptionHandler({ReaderException.class})
	public ResponseEntity<ApiResponse<String>> readerException(ReaderException e) {
		
		var response = apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);	
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), null);
		
		this.updateRequest(e.getRequestContext(), "Reader", e.getMessage(), null);
		
		return ResponseEntity.internalServerError().body(response);
	}
	
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ExceptionHandler({ProcessException.class})
	public ResponseEntity<ApiResponse<String>> processException(ProcessException e) {
		
		var response = apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), null);	
		
		sendExceptionEmail(e.getRequestContext(), e.getMessage(), e.getCause(), null);
		
		this.updateRequest(e.getRequestContext(), "Process", e.getMessage(), null);
		
		return ResponseEntity.internalServerError().body(response);
	}
	
	
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<ApiResponse<Map<String, String>>> handleValidationExceptions(MethodArgumentNotValidException ex) {
		ApiResponse<Map<String, String>> response = null;
	    Map<String, String> errors = new HashMap<>();
	    ex.getBindingResult().getAllErrors().forEach((error) -> {
	    	if(error instanceof FieldError) {
	    		String fieldName = ((FieldError) error).getField();
	    		String errorMessage = error.getDefaultMessage();
	    		errors.put(fieldName, errorMessage);
	    	} else if(error instanceof ObjectError) {	    		
	    		//String fieldName = error.getObjectName();
	    		String errorMessage = error.getDefaultMessage();
	    		errors.put("Payload", errorMessage);
	    	}
	    });
	    ErrorResponse error = new ErrorResponse(HttpStatus.BAD_REQUEST.toString(), errors.toString());
		response = ApiResponse.error(Status.ERROR, error);
	    return ResponseEntity.badRequest().body(response);
	}

}
