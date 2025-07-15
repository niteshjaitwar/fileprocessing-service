package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.ds.config.model.InputRule;
import com.adp.esi.digitech.file.processing.enums.RequestStatus;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.Metadata;
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.validation.service.HeaderValidationService;
import com.adp.esi.digitech.file.processing.validation.service.JSONMetadataValidationService;

import lombok.extern.slf4j.Slf4j;

@Service("jsonProcessorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class JSONProcessorService extends AbstractProcessorService<ProcessResponse> {

	/*
	 * 1. read
	 * 2. validations
	 * 		1. Json defalult validations
	 * 		2. Headers match validation
	 * 		3. Data validation rules
	 * 3. Transformer
	 * 4. Generator
	 * 5. Place files in location
	 */
	
	public void constructDefaults() throws ConfigurationException{
		super.constructDefaults(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
	}
	
	@Override
	public ProcessResponse process(RequestPayload request) throws IOException, ReaderException, ConfigurationException, ValidationException, TransformationException, GenerationException, ProcessException {
		
		log.info("JSONProcessorService -> process() Received JSON request for processing, uniqueId = {}", request.getUniqueId());
		
		this.initRequet(request);
		
		this.constructDefaults();
		
		this.validate(request);		
		
		String inputRulesJson = configurationData.getInputRules();		
		String outputRulesJson = configurationData.getOutputFileRules();
		String datasetRulesJson = configurationData.getDataRules();
		
		List<InputRule> inputRules = this.getInputRules(inputRulesJson);
		List<DataSetRules> dataSetRules = this.getDataSetRules(datasetRulesJson);
		
		Map<String, List<String>> headers = objectUtilsService.customReaderDynamicAutowireService.readJsonHeaders(request.getRawJsonPayload(), this.requestContext);
		
		log.info("JSONProcessorService -> process() Completed reading JSON Headers, uniqueId = {}, headers = {}",request.getUniqueId(), headers);
		
		var errors = headers.entrySet().parallelStream().map(entry -> {	
			try {
				log.info("JSONProcessorService -> process() Started Validating Headers for each category uniqueId = {}, key = {}",request.getUniqueId(), entry.getKey());			
				List<String> dbHeaders = columnRelationMap.get(entry.getKey()).parallelStream().map(columnRelation -> columnRelation.getColumnName()).collect(Collectors.toList());			
				objectUtilsService.customValidatorDynamicAutowireService.validate(HeaderValidationService.class, new Metadata(headers.get(entry.getKey()), dbHeaders), this.requestContext);
				log.info("JSONProcessorService -> process() Completed Validating Headers for each category uniqueId = {}, key = {}",request.getUniqueId(), entry.getKey());	
			} catch (MetadataValidationException e) {
				log.error("JSONProcessorService -> process() Failed Validating Headers for each category uniqueId = {}, key = {}, message = {}",request.getUniqueId(), entry.getKey(), e.getMessage());
				return new ErrorData(entry.getKey(), e.getMessage());
			}
			return null;
		}).filter(Objects::nonNull).collect(Collectors.toList());
		
		if(!errors.isEmpty()) {
			log.info("JSONProcessorService -> process() Failed validating headers, uniqueId = {}, errors = {}",request.getUniqueId(), errors);
			var metadataValidationException = new MetadataValidationException("Headers Validation Failed");
			metadataValidationException.setErrors(errors);
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		
		log.info("JSONProcessorService -> process() Completed validating headers, uniqueId = {}",request.getUniqueId());
		
		Map<String, List<DataMap>> dataMap = this.read(request);
		
		log.info("JSONProcessorService -> process() Completed reading JSON data, uniqueId = {}",request.getUniqueId());
		
		columnsToValidateMap = getSourceColumnsToValidate(inputRules);
		var dataSetMap = process(inputRules, dataMap, dataSetRules);
		
		/*	
		var dataSets = this.constructDataSets(inputRules, dataMap);
		applyExclusions(dataSets);
		
		log.info("JSONProcessorService -> process() Completed constructing dataSets, uniqueId = {}",request.getUniqueId());
		
		//Apply Data Exclusions
		
		var dataSetMap = this.process(dataSets, dataSetRules);		
		 */	
		
		log.info("JSONProcessorService -> process() Completed transforming dataSets, uniqueId = {}",request.getUniqueId());
		
		var sharedFiles = this.generate(dataSetMap, outputRulesJson);
		
		log.info("JSONProcessorService -> process() Completed constructing files, uniqueId = {}",request.getUniqueId());
		
		this.send(sharedFiles, fileProcessType);
		
		this.sendEmail(sharedFiles);
		
		this.updateRequest(request, RequestStatus.Completed);
		
		log.info("JSONProcessorService -> process() Completed processing, uniqueId = {}", request.getUniqueId());	
		return returnProcessResponse(sharedFiles);
	}
	
	@Override
	public void validate(RequestPayload request) throws ValidationException {		
		objectUtilsService.customValidatorDynamicAutowireService.validate(JSONMetadataValidationService.class, request.getRawJsonPayload(), this.requestContext);
		log.info("JSONProcessorService -> validate() Completed metadata validations, uniqueId = {}", request.getUniqueId());		
	}
	
	@Override
	public Map<String, List<DataMap>> read(RequestPayload request) throws ReaderException  {
		return objectUtilsService.customReaderDynamicAutowireService.readJsonData(request.getRawJsonPayload(), columnRelationMap, this.requestContext);	
	}

}
