package com.adp.esi.digitech.file.processing.exception;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.model.ApiResponse;
import com.adp.esi.digitech.file.processing.model.ErrorResponse;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.notification.model.Email;
import com.adp.esi.digitech.file.processing.notification.service.EmailService;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ControllerAdvice
@Order(Ordered.LOWEST_PRECEDENCE)
public class AutomationhubFileprocessingDefaultExpectionHandler  extends AbstractFileprocessingExpectionHandler {
	
	@Value("${fileprocessing.mail.to}")
	private String toEmail;
	
	@Value("${fileprocessing.mail.from}")
	private String fromEmail;
	
	@Value("${fileprocessing.mail.subject}")
	private String mailSubject;
	
	@Value("${fileprocessing.mail.old.subject}")
	private String oldMailSubject;
	
	@Value("${fileprocessing.mail.body}")
	private String mailBody;
	
	@Value("${fileprocessing.mail.mailbodysignature}")
	private String mailbodysignature;
	
	@Autowired
	private EmailService emailService;
	
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ExceptionHandler({IOException.class,Exception.class})
	public ResponseEntity<ApiResponse<String>> generationException(HttpServletRequest request, Exception exception) {
		ApiResponse<String> response = null;
		ErrorResponse error = new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.toString(), exception.getMessage());
		response = ApiResponse.error(Status.ERROR, error);
		
		
		var isEmailSend = false;
		if("POST".equalsIgnoreCase(request.getMethod())) {
			if(request.getRequestURI().equalsIgnoreCase("/ahub/fileprocessing/v2/json/process") 
					|| request.getRequestURI().equalsIgnoreCase("/ahub/fileprocessing/v2/shared-files/process")) {		
				try {
					var body = IOUtils.toString(request.getReader());
					log.info("AutomationhubFileprocessingDefaultExpectionHandler - body = {}", body);
					if(ValidationUtil.isHavingValue(body) && ValidationUtil.isValidJson(body)) {
						JSONObject jsonbody = new JSONObject(body);
						if(jsonbody != null && jsonbody.has("uniqueId") && !jsonbody.isNull("uniqueId")) {
							RequestContext requestContext = new RequestContext();
							requestContext.setBu(jsonbody.getString("bu"));
							requestContext.setPlatform(jsonbody.getString("platform"));
							requestContext.setDataCategory(jsonbody.getString("dataCategory"));
							requestContext.setUniqueId(jsonbody.getString("uniqueId"));						
							requestContext.setSaveFileLocation(jsonbody.getString("saveFileLocation"));
							
							sendExceptionEmail(requestContext, exception.getMessage(), exception.getCause(), null);	
							
							isEmailSend = true;
							this.updateRequest(requestContext, "Generic", exception.getMessage(), null);
						}
					}
				} catch (Exception e) {				
					log.error("AutomationhubFileprocessingDefaultExpectionHandler - generationException(), error = {}", e.getMessage());
				}
			} else if(request.getRequestURI().equalsIgnoreCase("/ahub/fileprocessing/v2/json") 
					|| request.getRequestURI().equalsIgnoreCase("/ahub/fileprocessing/v2/excel/upload")) {
				try {
					RequestContext requestContext = new RequestContext();
					requestContext.setBu(request.getParameter("bu"));
					requestContext.setPlatform(request.getParameter("platform"));
					requestContext.setDataCategory(request.getParameter("dataCategory"));
					requestContext.setUniqueId(request.getParameter("uniqueId"));			
					requestContext.setSaveFileLocation(request.getParameter("saveFileLocation"));
					
					sendExceptionEmail(requestContext, exception.getMessage(), exception.getCause(), null);	
					
					isEmailSend = true;
					this.updateRequest(requestContext, "Generic", exception.getMessage(), null);
				} catch (Exception e) {				
					log.error("AutomationhubFileprocessingDefaultExpectionHandler - generationException(), error = {}", e.getMessage());
				}
			}
		}
		if(!isEmailSend) {
			Email e = new Email();
			e.setFrom(fromEmail);
			e.setTo(toEmail);
			e.setSubject(oldMailSubject);
			e.setBody(mailBody+"<b>Exception:</b> "+ exception.getMessage() + mailbodysignature);
			emailService.send(e);
		}
		return ResponseEntity.internalServerError().body(response);
	}
}
