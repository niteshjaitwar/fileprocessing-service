package com.adp.esi.digitech.file.processing.autowire.service;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;


import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.processor.service.IProcessorService;

@Service("customProcessorDynamicAutowireService")
public class CustomProcessorDynamicAutowireService {
	
	private final WebApplicationContext webApplicationContext;	
	
	@Autowired
	public CustomProcessorDynamicAutowireService(WebApplicationContext webApplicationContext) {
		this.webApplicationContext = webApplicationContext;
	}
	
	public <T extends IProcessorService<ProcessResponse>> ProcessResponse process(Class<T> type, RequestPayload data, RequestContext requestContext, ProcessType fileProcessType) throws IOException, ReaderException, ConfigurationException, ValidationException, TransformationException, GenerationException, ProcessException {
		IProcessorService<ProcessResponse> processorService = webApplicationContext.getBean(type);
		processorService.setRequestContext(requestContext);
		processorService.setFileProcessType(fileProcessType);
		return processorService.process(data);
	}
	
	public <T extends IProcessorService<Void>> void processAsync(Class<T> type, RequestPayload data, RequestContext requestContext) throws IOException, ReaderException, ConfigurationException,
	ValidationException, TransformationException, GenerationException, ProcessException {
			IProcessorService<Void> processorService = webApplicationContext.getBean(type);
			processorService.setRequestContext(requestContext);
			processorService.process(data);
	}

}
