package com.adp.esi.digitech.file.processing.processor.service;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

//import com.adp.esi.digitech.file.processing.autowire.service.CustomFilterDynamicAutowireService;
import com.adp.esi.digitech.file.processing.autowire.service.CustomGeneratorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.autowire.service.CustomReaderDynamicAutowireService;
//import com.adp.esi.digitech.file.processing.autowire.service.CustomTransformDynamicAutowireService;
import com.adp.esi.digitech.file.processing.autowire.service.CustomValidatorDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.service.DatastudioConfigurationService;
import com.adp.esi.digitech.file.processing.dvts.service.DVTSProcessingService;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.notification.service.EmailNotificationService;
import com.adp.esi.digitech.file.processing.request.service.FPSRequestService;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

@Service
@Data
public class ProcessorObjectUtilsService {
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	CustomValidatorDynamicAutowireService customValidatorDynamicAutowireService;
	
	@Autowired
	CustomReaderDynamicAutowireService customReaderDynamicAutowireService;
	
	@Autowired
	DatastudioConfigurationService dataStudioConfigurationService;	
	
	@Autowired
	DVTSProcessingService dvtsProcessingService;
	
	//@Autowired
	//CustomTransformDynamicAutowireService customTransformDynamicAutowireService;
	
	@Autowired
	CustomGeneratorDynamicAutowireService customGeneratorDynamicAutowireService;
	
	@Autowired
	ObjectProvider<IFileService> objectProviderSharedFilesService;
	
	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	EmailNotificationService emailNotificationService;
	
	@Autowired
	FPSRequestService fpsRequestService;
	
	@Autowired
	ObjectProvider<Column> columnObjProvider;
	
	@Autowired
	ModelMapper modelMapper;
	
}
