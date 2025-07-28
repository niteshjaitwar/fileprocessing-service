package com.adp.esi.digitech.file.processing.autowire.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;

import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.validation.service.IValidationService;

@Service("customValidatorDynamicAutowireService")
public class CustomValidatorDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;
	
	@Autowired
	public CustomValidatorDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	
	public <T extends IValidationService<V>,V> void validate(Class<T> type, V data, RequestContext requestContext) throws ValidationException {
		IValidationService<V> validatorService = webApplicationContext.getBean(type);
		validatorService.setRequestContext(requestContext);
		validatorService.validate(data);
	}
}
