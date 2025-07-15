package com.adp.esi.digitech.file.processing.processor.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.InputRule;
import com.adp.esi.digitech.file.processing.ds.config.model.OutputRule;
import com.adp.esi.digitech.file.processing.ds.config.model.Reference;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.ds.model.ConfigurationData;
import com.adp.esi.digitech.file.processing.dvts.dto.DVTSProcessingResponseDTO;
import com.adp.esi.digitech.file.processing.dvts.dto.DataPayload;
import com.adp.esi.digitech.file.processing.enums.ProcessType;
import com.adp.esi.digitech.file.processing.enums.RelationshipType;
import com.adp.esi.digitech.file.processing.enums.RequestStatus;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.enums.ValidationType;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.DataValidationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.generator.service.CSVGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.ExcelGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.PdfGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.StAXXMLGeneratorService;
import com.adp.esi.digitech.file.processing.generator.service.TextGeneratorService;
import com.adp.esi.digitech.file.processing.model.Column;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.DataSet;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.FPSRequest;
import com.adp.esi.digitech.file.processing.model.Metadata;
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.model.Row;
import com.adp.esi.digitech.file.processing.model.SharedFile;
import com.adp.esi.digitech.file.processing.notification.model.EmailNotificationData;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;
import com.adp.esi.digitech.file.processing.validation.service.HeaderValidationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public abstract class AbstractProcessorService<T> implements IProcessorService<T> {
	
	RequestContext requestContext;
	
	ProcessorObjectUtilsService objectUtilsService;
	
	ConfigurationData configurationData;
	
	Map<String, List<ColumnRelation>> columnRelationMap;
	
	Map<String, List<String>> columnsToValidateMap;
	
	public ProcessType fileProcessType;
	
	@Value("${default.app.code}")
	public String appCode;
	
	@Value("${request.window.batch.size}")
	int windowBatchSize;
	
	@Override
	public void setFileProcessType(ProcessType fileProcessType) {
		this.fileProcessType = fileProcessType;
	}
	
	@Override
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}
	
	@Autowired
	public void setUtilsService(ProcessorObjectUtilsService objectUtilsService) {
		this.objectUtilsService = objectUtilsService;
	}
	
	public void initRequet(RequestPayload request) {
		FPSRequest dataProcessingRequest = new FPSRequest();
		dataProcessingRequest.setUniqueId(requestContext.getUniqueId());
		dataProcessingRequest.setUuid(requestContext.getRequestUuid());
		dataProcessingRequest.setBu(requestContext.getBu());
		dataProcessingRequest.setPlatform(requestContext.getPlatform());
		dataProcessingRequest.setDataCategory(requestContext.getDataCategory());
		dataProcessingRequest.setSaveFileLocation(requestContext.getSaveFileLocation());
		if(request.getRawJsonPayload() != null)	{
			dataProcessingRequest.setRequestPayload(request.getRawJsonPayload().toString());
		} else if(request.getDocuments() != null) {
			try {
				String json = objectUtilsService.objectMapper.writeValueAsString(request.getDocuments());
				dataProcessingRequest.setRequestPayload(json);
			} catch (JsonProcessingException e) {
				log.error("AbstractProcessorService - initRequet() failed to parse json. uniqueId = {},  Error Message = {}", requestContext.getRequestUuid(), e.getMessage());
			}
		}
			
		dataProcessingRequest.setStatus("Submitted");		
		dataProcessingRequest.setSourceType(request.getSource());
		
		if(ValidationUtil.isHavingValue(request.getUseremail())) {
			dataProcessingRequest.setCreatedBy(request.getUseremail());
		} else {;
			dataProcessingRequest.setCreatedBy("DVTS-Admin");
		}
		
		dataProcessingRequest.setCreatedDate(new Date());;
		
		objectUtilsService.fpsRequestService.add(dataProcessingRequest);
	}
	
	public void updateRequest(RequestPayload request, RequestStatus status) {
		FPSRequest dataProcessingRequest = new FPSRequest();
		dataProcessingRequest.setUniqueId(requestContext.getUniqueId());
		dataProcessingRequest.setUuid(requestContext.getRequestUuid());
		dataProcessingRequest.setBu(requestContext.getBu());
		dataProcessingRequest.setPlatform(requestContext.getPlatform());
		dataProcessingRequest.setDataCategory(requestContext.getDataCategory());
		dataProcessingRequest.setStatus(status.getRequestStatus());
		
		objectUtilsService.fpsRequestService.update(dataProcessingRequest);
	}
	
	
	public void constructDefaults(String bu, String platform, String dataCategory) throws ConfigurationException {
		log.info("AbstractProcessorService - constructDefaults() Started loading Configuration Data, Column Relations, Validation Rules, Transformation Rules and LOV Metadata. uniqueId = {}", requestContext.getUniqueId());
		var errorMessages = new ArrayList<ErrorData>();
		try {
			
			CompletableFuture<ConfigurationData> configurationDataCompletable = CompletableFuture.supplyAsync(() -> objectUtilsService.dataStudioConfigurationService.findConfigurationDataBy(bu, platform, dataCategory));
			CompletableFuture<Map<String,List<ColumnRelation>>> columnRelationDataCompletable = CompletableFuture.supplyAsync(() -> objectUtilsService.dataStudioConfigurationService.findAllColumnRelationsMapBy(bu, platform, dataCategory));
			
			CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(configurationDataCompletable, columnRelationDataCompletable);
			combinedFuture.join();			
			
			configurationData  = configurationDataCompletable.get();			
			columnRelationMap = columnRelationDataCompletable.get();	
			
			if(configurationData == null)
				errorMessages.add(new ErrorData("Configuration Error","No Configuration found for given request"));
			
			if(configurationData != null && !ValidationUtil.isHavingValue(configurationData.getInputRules()))
				errorMessages.add(new ErrorData("Configuration Error","No Input Rules found for given request"));
			if(configurationData != null && ValidationUtil.isHavingValue(configurationData.getInputRules()) && !ValidationUtil.isValidJsonArray(configurationData.getInputRules()))
				errorMessages.add(new ErrorData("Configuration Error","Configured Input Rules are invalid for given request"));
			if(configurationData != null && !ValidationUtil.isHavingValue(configurationData.getOutputFileRules()))
				errorMessages.add(new ErrorData("Configuration Error","No Output Rules found for given request "));
			if(configurationData != null && ValidationUtil.isHavingValue(configurationData.getOutputFileRules()) && !ValidationUtil.isValidJsonArray(configurationData.getOutputFileRules()))
				errorMessages.add(new ErrorData("Configuration Error","Configured Output Rules are invalid for given request"));
				
			if(columnRelationMap == null || columnRelationMap.isEmpty())
				errorMessages.add(new ErrorData("Configuration Error","No Column Relations found for given request"));
			
			if(errorMessages != null && !errorMessages.isEmpty()) {
				var configurationException = new ConfigurationException("Configurations are not proper for the given request");
				configurationException.setErrors(errorMessages);
				configurationException.setRequestContext(requestContext);
				throw configurationException;
			}
			
			log.info("AbstractProcessorService - constructDefaults() completed loading Configuration Data, Column Relations. uniqueId = {}", requestContext.getUniqueId());
		} catch (ConfigurationException e) {
			log.error("AbstractProcessorService - constructDefaults() missing configurations for the request. uniqueId = {},  Error Message = {}", requestContext.getRequestUuid(), e.getMessage());
			e.setErrors(errorMessages);
			e.setRequestContext(requestContext);
			throw e;
		} catch(InterruptedException e) {
			log.error("AbstractProcessorService - constructDefaults() failed to load configurations for the request. uniqueId = {},  Error Message = {}", requestContext.getRequestUuid(), e.getMessage());
			Thread.currentThread().interrupt();
			var configurationException = new ConfigurationException(e.getMessage(), e.getCause());
			configurationException.setErrors(errorMessages);
			configurationException.setRequestContext(requestContext);
			throw configurationException;
		} catch (Exception e) {
			log.error("AbstractProcessorService - constructDefaults() failed to load configurations for the request. uniqueId = {},  Error Message = {}", requestContext.getRequestUuid(), e.getMessage());
			var configurationException = new ConfigurationException(e.getMessage(), e.getCause());
			configurationException.setErrors(errorMessages);
			configurationException.setRequestContext(requestContext);
			throw configurationException;
		}
	}

	private <V> void validateJson(String json, Class<V> type) throws ConfigurationException {
		if (!ValidationUtil.isHavingValue(json)) {
			var configurationException = new ConfigurationException("Configuration Error - No " + type.getName() + " Rules configured");
			configurationException.setRequestContext(requestContext);
			throw configurationException;
		}
	}
	
	private <V> List<V> getRules(String json , TypeReference<List<V>> type) throws ConfigurationException {
		try {
			return objectUtilsService.objectMapper.readValue(json, type);
		} catch (JsonProcessingException e) {
			log.error("AbstractProcessorService - getRules(), Failed to process rules, rule = {}, uniqueId = {}, Error Message = {}", type.getClass().getName(), requestContext.getRequestUuid(), e.getMessage());
			var configurationException = new ConfigurationException("Configuration Error - Failed to process " + type.getClass().getName() + " Rules", e.getCause());
			configurationException.setRequestContext(requestContext);
			throw configurationException;
		}		
	}	
	public List<InputRule> getInputRules(String json) throws ConfigurationException {
		validateJson(json, InputRule.class);
		return getRules(json, new TypeReference<List<InputRule>>() {});	
	}
	
	public List<OutputRule> getOutputRules(String outputRulesJson) throws ConfigurationException {
		validateJson(outputRulesJson, OutputRule.class);
        return getRules(outputRulesJson, new TypeReference<List<OutputRule>>() {});
	}
	
	public Map<String,FileMetaData> getFileMetaDataMap(String json) throws ConfigurationException {
		validateJson(json, FileMetaData.class);		
		return getRules(json, new TypeReference<List<FileMetaData>>() {}).stream().collect(Collectors.toMap(FileMetaData::getSourceKey, Function.identity()));
	}
	
	public Map<String,FileMetaData> getFileMetaDataMap(Map<String,FileMetaData> fileMetaDataMap, String fileName) {
		return fileMetaDataMap.entrySet().stream()
		  .filter(entry -> entry.getKey().startsWith(fileName) || entry.getKey().equals(fileName))
		  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));	
	}
	
	public  List<DataSetRules> getDataSetRules(String json) {
		if (!ValidationUtil.isHavingValue(json))
			return null;
		return getRules(json, new TypeReference<List<DataSetRules>>() {});
	}
	
	public List<String> getProcessSteps(String json) {
		if (!ValidationUtil.isHavingValue(json)) {			
			var steps = new ArrayList<String>();
			steps.add(ValidationType.client.getValidationType());
			
			return steps;
		}
		return getRules(json, new TypeReference<List<String>>() {});
	
	}
	
	public Map<String, FileMetaData> getFileMetaDataMap(List<InputRule> inputRules) {		
		var map = new HashMap<String, FileMetaData>();		
		inputRules.stream().forEach(source -> {
			map.putIfAbsent(source.getSourceKey(), new FileMetaData(source.getSourceKey(), null, null, null, source.getHeaderIndex()));			
			if(Objects.nonNull(source.getReferences()) && !source.getReferences().isEmpty())
				source.getReferences().stream().forEach(reference -> map.putIfAbsent(reference.getSourceKey(), new FileMetaData(reference.getSourceKey(), null, null, null, reference.getHeaderIndex())));
		});		
		return map;
	}

	public DataSet<DataMap> constructDataSet(InputRule inputRule, Map<String, List<DataMap>> dataMap) {			
			var sourceKey= inputRule.getSourceKey();
			var sourceData = dataMap.get(sourceKey);
			var tempData = sourceData.stream().map(SerializationUtils::clone).collect(Collectors.toList());
			
			if(tempData == null || tempData.isEmpty()) {
				var validationException = new ValidationException("No data found in source");
				var errors = List.of(new ErrorData(sourceKey, "Empty data"));
				validationException.setErrors(errors);
				validationException.setRequestContext(requestContext);
				throw validationException;
				
			}
			var dataSetName = inputRule.getDataSetName();		
			var dataSetId = inputRule.getDataSetId();			
			var references = inputRule.getReferences();
			if(references != null && !references.isEmpty()) {
				for (Reference reference : references) {
					
					var relationship = reference.getRelationship();					
					relationship = Objects.nonNull(relationship) ? relationship : RelationshipType.OnetoOne;
					
					var rsourceKey= reference.getSourceKey();				
					var referenceData = dataMap.get(rsourceKey);	
					var keyIdentifiers = reference.getKeyIdentifiers();	
					var referenceType = reference.getType();				
					
					var referenceDataMap = (Objects.nonNull(referenceData) && Objects.nonNull(keyIdentifiers) && !keyIdentifiers.isEmpty()) ? referenceData.parallelStream().collect(Collectors.toMap(rRow -> {
						return keyIdentifiers.stream().map(keyIdentifier -> String.valueOf(rRow.getColumns().get(UUID.fromString(keyIdentifier.getReferenceIdentifier())))).collect(Collectors.joining("_"));
					}, Function.identity(), (existing, replacement) -> existing)) : null;
					
					 var emptyCollumnMap = new HashMap<UUID, String>();
					if("dataset".equalsIgnoreCase(referenceType)) {
						var referenceFirstRow = referenceData.get(0);
						var rColumns = referenceFirstRow.getColumns();
						rColumns.forEach((key, value) -> {
							emptyCollumnMap.put(key, null);
						});					
					} else {
						var rColumnRealtions = columnRelationMap.get(rsourceKey);					   
					   
					    rColumnRealtions.forEach(relation -> {
					    	emptyCollumnMap.put(UUID.fromString(relation.getUuid()), null);
					    });
					}
					
					switch (relationship) {
						case OnetoOne:
							tempData = tempData.parallelStream().map(sRow -> {								
								if(Objects.nonNull(referenceDataMap)) {								
									var sKeyIdentifier = keyIdentifiers.stream().map(keyIdentifier -> String.valueOf(sRow.getColumns().get(UUID.fromString(keyIdentifier.getParentIdentifier())))).collect(Collectors.joining("_"));
									
									if(referenceDataMap.containsKey(sKeyIdentifier)) {
										var tempRow = SerializationUtils.clone(referenceDataMap.get(sKeyIdentifier));
										sRow.getColumns().putAll(tempRow.getColumns());
										return sRow;
									}
								}								
								//var map = emptyCollumnMap.values().stream().map(item -> (Column) item.clone()).collect(Collectors.toMap(Column::getUuid, Function.identity()));
								sRow.getColumns().putAll(emptyCollumnMap);
								return sRow;
							}).collect(Collectors.toList());
							
							break;	
						case AlltoOne:
							
							var rowNumber = reference.getDataRowNumber();
							var row = Objects.isNull(referenceData) || referenceData.isEmpty() ? new DataMap(emptyCollumnMap) : referenceData.size() >= rowNumber ?	referenceData.get(rowNumber-1) : referenceData.get(0);				
							
							tempData = tempData.parallelStream().map(sRow -> {
								var tempRow = SerializationUtils.clone(row);
								sRow.getColumns().putAll(tempRow.getColumns());
								return sRow;
							}).collect(Collectors.toList());
							
							break;						
						case ManytoOne: case OnetoMany:
							break;
					}		
				}
			}
			DataSet<DataMap> dataSet = new DataSet<>();
			dataSet.setId(dataSetId);
			dataSet.setName(dataSetName);			
			dataSet.setData(tempData);
			dataSet.setColumnsToValidate(columnsToValidateMap.get(dataSetId));
			return dataSet;
	}
	
	public List<DataSet<DataMap>> constructDataSets(List<InputRule> inputRules, Map<String, List<DataMap>> dataMap) {		
		return inputRules.stream().map(inputRule -> {			
			return constructDataSet(inputRule, dataMap);
		}).collect(Collectors.toList());
	}
	
	public Mono<DVTSProcessingResponseDTO> process(DataSet<DataMap> dataSet, DataSetRules dataSetRule) {
		var payload = new DataPayload();
		payload.setRequestContext(requestContext);
		payload.setDatasetId(dataSet.getId());
		payload.setDatasetName(dataSet.getName());
		payload.setData(dataSet.getData());
		payload.setDataSetRule(dataSetRule);
		payload.setColumnsToValidate(dataSet.getColumnsToValidate());
		return objectUtilsService.dvtsProcessingService.processData(payload).map(response -> {
			response.setDataSetId(dataSet.getId());		
			response.setDataSetName(dataSet.getName());
			return response;
		});
	}
	
	public Map<String, DataSet<Row>> process(List<DataSet<DataMap>> dataSets, List<DataSetRules> dataSetRules) throws DataValidationException, TransformationException, ProcessException {		
		if(dataSetRules == null || dataSetRules.isEmpty()) {
			log.info("AbstractProcessorService -> applyDataRules() No Data Rules found, uniqueId = {}", requestContext.getUniqueId());			
		}
		
		var dataSetRulesMap = (Objects.nonNull(dataSetRules) && !dataSetRules.isEmpty())? dataSetRules.stream().collect(Collectors.toMap(DataSetRules::getDataSetId, Function.identity())) : new HashMap<String, DataSetRules>();
	
		var monos = dataSets.parallelStream().map(dataSet -> {			
			return process(dataSet, dataSetRulesMap.get(dataSet.getId()));
		}).collect(Collectors.toList());	
		
		
		var responses = Flux.merge(monos)
				.window(windowBatchSize)
				.concatMap(batch -> batch.collectList())
				.flatMap(list -> {
					return Flux.fromIterable(list);
				}).collectList()
				.onErrorResume(e -> {
					var processException = new ProcessException("Failed to process request",e);
					processException.setRequestContext(requestContext);
					return Mono.error(processException);				
				}).block();
		
		/*
		var responses = Flux.merge(monos).collectList()
			.onErrorResume(e -> {
				var processException = new ProcessException("Failed to process request",e);
				processException.setRequestContext(requestContext);
				return Mono.error(processException);				
			}).block();
			*/
		handleDataValidationErrors(responses, Status.CAM_DATA_VALIDATION, "CAM");
		handleDataValidationErrors(responses, Status.CLIENT_DATA_VALIDATION, "client");
		
		responses.parallelStream()
			.filter(response -> response.getStatus().equals(Status.ERROR) || response.getStatus().equals(Status.DATA_TRANSFORMATION))
			.findAny().ifPresent(response -> {
				var processException = new ProcessException("Failed to process request, reason = " + response.getMessage());
				processException.setRequestContext(requestContext);
				throw processException;
			});
		
		return responses.parallelStream().map(response -> {
			
			var dataSet = new DataSet<Row>();
			dataSet.setId(response.getDataSetId());
			dataSet.setId(response.getDataSetId());
			dataSet.setData(response.getData());
			
			var targetDataFormatMap = objectUtilsService.dataStudioConfigurationService
				.findTargetDataFormatBy(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory(), dataSet.getId());
			
			dataSet.setTargetFormatMap(targetDataFormatMap);
			
			return dataSet;
		}).collect(Collectors.toMap(DataSet::getId, Function.identity()));
	}
	
	private void handleDataValidationErrors(List<DVTSProcessingResponseDTO> responses, Status status, String validationType) {
		var errorMap = responses.parallelStream()
				.filter(response -> response.getStatus().equals(status))
				.map(DVTSProcessingResponseDTO::getData)
				.flatMap(List::stream)
				.collect(Collectors.collectingAndThen(Collectors.toList(), this::constructErrorData));
		
		if(Objects.nonNull(errorMap) && !errorMap.isEmpty()) {
			var sharedFile = constructErrorFile(errorMap, validationType);
			var targetLocation = configurationData.getTargetLocation();
			var iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, 
					TargetLocation.SharePoint.getTargetLocation().equalsIgnoreCase(targetLocation) ? TargetLocation.SharePoint : TargetLocation.SharedDrive);
								
			
			CompletableFuture.runAsync(() -> iFileService.uploadFile(sharedFile));	
	
			var dataValidationException = new DataValidationException("Failed to Perform "+ validationType +" Data Validations, Please refer to exception file");
			//dataValidationException.setErrors(errors);
			dataValidationException.setRequestContext(requestContext);
			throw dataValidationException;
		}
	}
	
	
	
	public Mono<DVTSProcessingResponseDTO> validate(DataSet<DataMap> dataSet, ValidationType validationType) throws DataValidationException, ProcessException {
		var payload = new DataPayload();
		payload.setValidationType(validationType);
		payload.setRequestContext(requestContext);
		payload.setDatasetId(dataSet.getId());
		payload.setDatasetName(dataSet.getName());
		payload.setData(dataSet.getData());	
		payload.setBatchName(dataSet.getBatchName());
		payload.setBatchSize(dataSet.getBatchSize());
		return objectUtilsService.dvtsProcessingService.processValidations(payload);
	}


	public Mono<DVTSProcessingResponseDTO> transform(DataSet<DataMap> dataSet, DataSetRules dataSetRule, Map<String, String> dynamicClauseValues) throws TransformationException {
		var payload = new DataPayload();		
		payload.setRequestContext(requestContext);
		payload.setDatasetId(dataSet.getId());
		payload.setDatasetName(dataSet.getName());
		payload.setData(dataSet.getData());
		payload.setDataSetRule(dataSetRule);
		payload.setClause(dynamicClauseValues);
		payload.setBatchName(dataSet.getBatchName());
		payload.setBatchSize(dataSet.getBatchSize());
		return objectUtilsService.dvtsProcessingService.processTransformations(payload);
		
	}	
	

	public List<SharedFile> generate(Map<String, DataSet<Row>> dataSetsMap, String outputRules) throws GenerationException {
		JSONArray outputFileRulesJsonArray = new JSONArray(outputRules);
		return IntStream.range(0, outputFileRulesJsonArray.length()).parallel().mapToObj(index -> {
			JSONObject outputFileRuleJson = outputFileRulesJsonArray.getJSONObject(index);
			
			//var isTransformationRequired = outputFileRuleJson.has("isTransformationRequired") && !outputFileRuleJson.isNull("isTransformationRequired")?  outputFileRuleJson.getString("isTransformationRequired") : "Y";
			var outputFileType = (String) outputFileRuleJson.get("outputFileType");
					
			
			//log.info("AbstractProcessorService -> generate() Started generating file, uniqueId = {}, dataSetName = {}, outputFileType = {}, dataSet = {}", requestContext.getUniqueId(),dataSetName, outputFileType, dataSet);
			
			SharedFile sharedFile = new SharedFile();
			var currentAppcode = ValidationUtil.isHavingValue(configurationData.getAppCode()) ? configurationData.getAppCode() : appCode;
			sharedFile.setAppCode(currentAppcode);
			
			if(TargetLocation.SharePoint.getTargetLocation().equalsIgnoreCase(configurationData.getTargetLocation())) {
				sharedFile.setPath(requestContext.getSaveFileLocation());
				sharedFile.setLocation(TargetLocation.SharePoint);
			} else {
				var targetFolderPath = objectUtilsService.fileUtils.getTargetFolderPath(requestContext.getSaveFileLocation(), currentAppcode);
				sharedFile.setPath(targetFolderPath);
				sharedFile.setLocation(TargetLocation.SharedDrive);
			}
			
			Row firstRowData = null;
			if(outputFileRuleJson.has("dataSetName") && !outputFileRuleJson.isNull("dataSetName")) {
				var dataSetName = (String) outputFileRuleJson.get("dataSetName");
				var dataSet = dataSetsMap.get(dataSetName);
				if(Objects.nonNull(dataSet) && Objects.nonNull(dataSet.getData()) && !dataSet.getData().isEmpty())
					firstRowData = dataSet.getData().get(0);	
			}
					
			var fileName = objectUtilsService.fileUtils.constructFileName(requestContext.getUniqueId(), outputFileRuleJson.getJSONObject("fileName"), firstRowData);
			
			sharedFile.setName(fileName + "." + outputFileType.toLowerCase());
			
			switch (outputFileType.toLowerCase()) {
				case "txt":	
					var dataJsonArray = outputFileRuleJson.getJSONArray("data");
					var txtDataMap = IntStream.range(0, dataJsonArray.length()).parallel().mapToObj(dataIndex -> {
						var dataRowJson = dataJsonArray.getJSONObject(dataIndex);
						var dataDatasetName = (String) dataRowJson.get("dataSetName");
						return dataSetsMap.get(dataDatasetName);
					}).collect(Collectors.toMap(DataSet::getId, Function.identity(), (first, second) -> first));					
					sharedFile.setBytes(objectUtilsService.customGeneratorDynamicAutowireService.generate(TextGeneratorService.class, outputFileRuleJson, txtDataMap, this.requestContext));
					
					break;
				case "xml":	
					//var dataSetName = (String) outputFileRuleJson.get("dataSetName");
					//var dataSet = dataSetsMap.get(dataSetName);
					sharedFile.setBytes(objectUtilsService.customGeneratorDynamicAutowireService.generate(StAXXMLGeneratorService.class, outputFileRuleJson, dataSetsMap, this.requestContext));
					break;
				case "xlsx":					
					var sheetsJsonArray = outputFileRuleJson.getJSONArray("sheets");
					var xlsxSheetsDataMap= IntStream.range(0, sheetsJsonArray.length()).parallel().mapToObj(sheetIndex -> {
						var sheetJson = sheetsJsonArray.getJSONObject(sheetIndex);
						var sheetDatasetName = (String) sheetJson.get("dataSetName");
						return dataSetsMap.get(sheetDatasetName);
					}).collect(Collectors.toMap(DataSet::getId, Function.identity(), (first, second) -> first));
					//Collectors.toMap(DataSet::getId, DataSet::getTransformedData, (first, second) -> first)
					
					sharedFile.setBytes(objectUtilsService.customGeneratorDynamicAutowireService.generate(ExcelGeneratorService.class, outputFileRuleJson, xlsxSheetsDataMap, this.requestContext));
					break;
				case "pdf":
					sharedFile.setBytes(objectUtilsService.customGeneratorDynamicAutowireService.generate(PdfGeneratorService.class, outputFileRuleJson, dataSetsMap, this.requestContext));
					break;
				case "csv":
					sharedFile.setBytes(objectUtilsService.customGeneratorDynamicAutowireService.generate(CSVGeneratorService.class, outputFileRuleJson, dataSetsMap, this.requestContext));
					break;
			}
			return sharedFile;
		}).collect(Collectors.toList());		
	}
	
	public void send(List<SharedFile> sharedFiles, ProcessType processType) {
		switch (processType) {
            case sync:
                send(sharedFiles);
                break;
            case async:
                sendAsync(sharedFiles);
                break;
            default:
            	break;
		}
	}

	
	public void send(List<SharedFile> sharedFiles) throws ProcessException {
		if(TargetLocation.SharePoint.getTargetLocation().equalsIgnoreCase(configurationData.getTargetLocation())) {
			var iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharePoint);
			sharedFiles.parallelStream().map(sharedFile -> iFileService.uploadFile(sharedFile))
					.filter(response -> response.getStatus().equalsIgnoreCase("Failed")).findAny()
					.ifPresent(response -> {
						var error = new IOException("Failed to upload file to SharePoint, reason = " + response.getReason());
						var processException = new ProcessException("Failed to upload file to SharePoint", error);
						processException.setRequestContext(requestContext);
						throw processException;
					});
			return;
		}
		
		var iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);
		sharedFiles.parallelStream().map(sharedFile -> iFileService.uploadFile(sharedFile))
		.filter(response -> response.getStatus().equalsIgnoreCase("Failed")).findAny()
		.ifPresent(response -> {
			var error = new IOException("Failed to upload file to SharedDrive, reason = " + response.getReason());
			var processException = new ProcessException("Failed to upload file to SharedDrive", error);
			processException.setRequestContext(requestContext);
			throw processException;
		});;
		
	}
	
	public void sendAsync(List<SharedFile> sharedFiles) {
		if(TargetLocation.SharePoint.getTargetLocation().equalsIgnoreCase(configurationData.getTargetLocation())) {
			var iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharePoint);
			sharedFiles.forEach(sharedFile -> {
				CompletableFuture.runAsync(() -> iFileService.uploadFile(sharedFile));
			});
			return;
		}
		var iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);
		sharedFiles.forEach(sharedFile -> {
			CompletableFuture.runAsync(() -> iFileService.uploadFile(sharedFile));
		});		
	}
	
	
	@SuppressWarnings("unused")
	private Map<String, List<Row>> constructErrorData2(List<Row> errorRows) {
	    var superMap = new HashMap<String, List<Row>>();
	    
	    var map = columnRelationMap.entrySet().parallelStream().flatMap(entry -> entry.getValue().parallelStream()).collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), ColumnRelation::getSourceKey));

	    errorRows.parallelStream()
	        .flatMap(row -> row.getColumns().values().parallelStream())
	        .collect(Collectors.groupingBy(column -> map.get(column.getUuid())))
	        .forEach((key, columns) -> {
	            var rowObj = new Row(columns.parallelStream().collect(Collectors.toMap(Column::getUuid, Function.identity())));
	            superMap.computeIfAbsent(key, k -> new ArrayList<>()).add(rowObj);
	        });

	    return superMap;
	}
	
	
	public Map<String, List<Row>> constructErrorData(List<Row> errorRows) {
		var map = columnRelationMap.entrySet().parallelStream().flatMap(entry -> entry.getValue().parallelStream()).collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), ColumnRelation::getSourceKey));
		
		var errorswithSourceKey = errorRows.parallelStream().map(row -> {
			return row.getColumns().values().parallelStream().collect(Collectors.groupingBy(column -> map.get(column.getUuid())));
		}).collect(Collectors.toList());
			
			var superMap = new HashMap<String, List<Row>>();
			
			for(Map<String, List<Column>> item : errorswithSourceKey) {
				for (Map.Entry<String,List<Column>> entry : item.entrySet())  {
					var columnsGroup = entry.getValue().parallelStream().collect(Collectors.toMap(Column::getUuid, Function.identity()));
					var rowObj = new Row(columnsGroup);
					
					if(superMap.containsKey(entry.getKey())) {
						var rows = superMap.get(entry.getKey());
						rows.add(rowObj);
						superMap.put(entry.getKey(), rows);
						
					} else {
						var rows = new ArrayList<Row>();
						rows.add(rowObj);
						superMap.put(entry.getKey(), rows);
					}
				}
			}			
			
		return superMap;
	}
	
	public SharedFile constructErrorFile(Map<String, List<Row>> sourceKeyMap, String exceptionType) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try(XSSFWorkbook workbook = new XSSFWorkbook()) {
			sourceKeyMap.entrySet().forEach(entity -> {
				//var validationRulesMap = getValidationRulesBy(exceptionType, entity.getKey());
				var sourceKeyColumnRelations = columnRelationMap.get(entity.getKey());
				var sourceKeyColumnRelationsMap = sourceKeyColumnRelations.parallelStream().collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), Function.identity()));
				var rules = sourceKeyColumnRelations.stream().filter(item -> "Y".equalsIgnoreCase(item.getColumnRequiredInErrorFile()))
				.collect(Collectors.toList());
				
				LinkedHashMap<Integer, ColumnRelation> rulesMap = new LinkedHashMap<>();
				
				int i = 0; 
		        while (i < rules.size()) {
		        	rulesMap.put(i, rules.get(i));
		            i++;
		        } 
				log.info("rules = {}", rules);
				XSSFSheet sheet = workbook.createSheet(entity.getKey());
				org.apache.poi.ss.usermodel.Row rowHeader = sheet.createRow(0);
				
				if(!rulesMap.isEmpty()) {				
					IntStream.range(0, rulesMap.size()).forEach(headerIndex -> {
						var columnName = rulesMap.get(headerIndex).getColumnName();
						rowHeader.createCell(headerIndex).setCellValue(columnName);
					});	
				}
				var headerIndex = rulesMap.size();
				rowHeader.createCell(headerIndex).setCellValue("Field with error");
				rowHeader.createCell(headerIndex + 1).setCellValue("Field Value");
				rowHeader.createCell(headerIndex + 2).setCellValue("Error Message");
				
				var rows = entity.getValue();
				var dataRowIndex = 1;
				
				for(Row row : rows) {
					var columns = row.getColumns().values();
					for(Column column: columns) {
						if(Objects.nonNull(column.getErrors()) && !column.getErrors().isEmpty()) {
							var errors = column.getErrors();
							for(String errorMessage: errors) {
								org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(dataRowIndex);
								if(!rulesMap.isEmpty()) {				
									IntStream.range(0, rulesMap.size()).forEach(columnIndex -> {
										var columnUuid = UUID.fromString(rulesMap.get(columnIndex).getUuid());
										var tempColumn = row.getColumns().get(columnUuid);
										excelRow.createCell(columnIndex).setCellValue((Objects.nonNull(tempColumn) && Objects.nonNull(tempColumn.getValue())) ? tempColumn.getValue().toString() : "");
									});	
								}
								excelRow.createCell(headerIndex).setCellValue(sourceKeyColumnRelationsMap.get(column.getUuid()).getColumnName());
								excelRow.createCell(headerIndex + 1).setCellValue(ValidationUtil.isHavingValue(column.getSourceValue())? column.getSourceValue() : null);
								excelRow.createCell(headerIndex + 2).setCellValue(errorMessage);	
								dataRowIndex++;
							}
						}
					}
				}
				
			});
		
		
			workbook.write(bos);
		} catch (IOException e) {
			log.error("AbstractProcessorService -> constructErrorFile() Failed to create error exception file, uniqueId = {}, Error Message = {}", requestContext.getUniqueId(), e.getMessage());
			var processException = new ProcessException("Failed to create error file", e);
			processException.setRequestContext(requestContext);
			throw processException;
		}
		
		SharedFile sharedFile = new SharedFile();
		var currentAppcode = ValidationUtil.isHavingValue(configurationData.getAppCode()) ? configurationData.getAppCode() : appCode;
		sharedFile.setAppCode(currentAppcode);
		
		if(TargetLocation.SharePoint.getTargetLocation().equalsIgnoreCase(configurationData.getTargetLocation())) {
			sharedFile.setPath(requestContext.getSaveFileLocation());
			sharedFile.setLocation(TargetLocation.SharePoint);
		} else {
			var targetFolderPath = objectUtilsService.fileUtils.getTargetFolderPath(requestContext.getSaveFileLocation(), currentAppcode);
			sharedFile.setPath(targetFolderPath);
			sharedFile.setLocation(TargetLocation.SharedDrive);
		}
		
		
		
		var fileName = "";
		if(exceptionType.equals("CAM")) {
			fileName = requestContext.getUniqueId() + "_CAM_Exception_" + requestContext.getDataCategory() + ".xlsx";
		}else {
			fileName = requestContext.getUniqueId() + "_Exception_" + requestContext.getDataCategory() + ".xlsx";
		}
		sharedFile.setName(fileName);
		sharedFile.setBytes(bos.toByteArray());
		
		
		return sharedFile;
	}
	
	public void validate(Sheet sheet,int hearderIndex ,List<String> dbHeaders) throws MetadataValidationException {
		var headerRow = sheet.getRow(hearderIndex);
		var headersCellIterator = headerRow.cellIterator();
		var reqheaders = new ArrayList<String>();

		while (headersCellIterator.hasNext()) {
			var cell = headersCellIterator.next();
			
			var cellValue = !StringUtils.isBlank(cell.getStringCellValue()) ? cell.getStringCellValue().strip() : "";
			cellValue = cellValue.lines().collect(Collectors.joining());
			reqheaders.add(cellValue);
		}
		Collections.sort(reqheaders);
		Collections.sort(dbHeaders);

		objectUtilsService.customValidatorDynamicAutowireService.validate(HeaderValidationService.class,new Metadata(reqheaders, dbHeaders), this.requestContext);		
	}
	
	public void sendEmail(List<SharedFile> sharedFiles) {
		EmailNotificationData emailNotificationData = new EmailNotificationData();
		emailNotificationData.setRequestContext(requestContext);	
		var files = sharedFiles.parallelStream().map(item -> item.getPath() + item.getName()).collect(Collectors.toList());
		emailNotificationData.setFilesPath(files);
		try {
			objectUtilsService.emailNotificationService.sendEmailWithTemplateBody("", emailNotificationData);
		} catch (IOException ioException) {
			log.error("AbstractProcessorService -> sendEmail() Failed to send email, uniqueId = {}, Error Message = {}", requestContext.getUniqueId(), ioException.getMessage());
		}
	}
	
	public ProcessResponse returnProcessResponse(List<SharedFile> sharedFiles) {		
		var response = objectUtilsService.modelMapper.map(this.requestContext, ProcessResponse.class);
		response.setFiles(sharedFiles);
		return response;
		
	}
	
	public void applyExclusions(List<DataSet<DataMap>> dataSets) {
		var tempRules = columnRelationMap.entrySet().parallelStream().flatMap(entry -> entry.getValue().stream())
				.filter(cr -> ValidationUtil.isHavingValue(cr.getDataExclusionRules()))
				.collect(Collectors.toMap(cr -> UUID.fromString(cr.getUuid()), ColumnRelation::getDataExclusionRules));	
		
		dataSets.parallelStream().forEach(dataSet -> {
			applyExclusions(dataSet, tempRules);
		});
		
	}
	
	public void applyExclusions(DataSet<DataMap> dataSet, Map<UUID, String> exclusionRulesMap) {
		if (Objects.nonNull(exclusionRulesMap) && !exclusionRulesMap.isEmpty()) {
			// log.info("DataTransformationProcessingService - process(), Found Data Exclusion Rules: {}", tempRules.size());
			var rows = dataSet.getData();
			rows.forEach(row -> {
				row.getColumns().keySet().parallelStream().forEach(columnKey -> {
					if (exclusionRulesMap.containsKey(columnKey)) {
						var exclusionRules = exclusionRulesMap.get(columnKey);
						var sourceValue = row.getColumns().get(columnKey);
						var dataSetId = UUID.fromString(dataSet.getId());
						var dataExclusionRulesJson = ValidationUtil.getDatasetRules(exclusionRules, dataSetId);

						if (Objects.nonNull(dataExclusionRulesJson) && dataExclusionRulesJson.has("values")
								&& !dataExclusionRulesJson.isNull("values")) {

							var jsonArrObj = dataExclusionRulesJson.getJSONArray("values");
							if (Objects.nonNull(jsonArrObj) && !jsonArrObj.isEmpty()) {
								IntStream.range(0, jsonArrObj.length()).filter(index -> {
									var jsonObj = jsonArrObj.getJSONObject(index);
									return jsonObj.has("sourceValue") && !jsonObj.isNull("sourceValue")
											? jsonObj.getString("sourceValue").equals(sourceValue)
											: false;
								}).findFirst().ifPresent(selectedIndex -> {
									var jsonObj = jsonArrObj.getJSONObject(selectedIndex);
									if (jsonObj.has("proxyValue"))
										if (jsonObj.isNull("proxyValue")) {
											row.getColumns().put(columnKey, null);
										} else {
											row.getColumns().put(columnKey, jsonObj.getString("proxyValue"));
										}
								});
							}

						}

					}
				});
			});
		}
	}
	
	
	public Map<String, List<DataMap>> read(RequestPayload request) throws ReaderException {
		return null;
	}


	public void validate(RequestPayload request) throws ValidationException {
		
	}
	

	public Map<String, DataSet<Row>> transfrom(List<DataSet<DataMap>> dataSets) throws TransformationException {		
		return null;
	}
		
	
	public void validate(List<DataSet<DataMap>> dataSets) throws DataValidationException {
			
	}
	
	public void clean(String fileLocation,String uniqueId) {		
		var iFileService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);
		CompletableFuture.runAsync(() -> iFileService.deleteFile(fileLocation,uniqueId,appCode));
	}
	
	public  Map<String, List<String>> getSourceColumnsToValidate(List<InputRule> inputRules) {
		var columnsToBeValidatedMap =  new  HashMap<String, List<String>>();		
		Predicate<String> sourcePredicate = type -> Objects.isNull(type) || "source".equals(type);
		
		inputRules.forEach(inputRule -> {
			
			var sourceType = inputRule.getType();
			var sourceKey = inputRule.getSourceKey();
			var dataSetId = inputRule.getDataSetId();
			if(sourcePredicate.test(sourceType)  && columnRelationMap.containsKey(sourceKey)) {
				columnsToBeValidatedMap.computeIfAbsent(dataSetId, key -> new ArrayList<String>()).addAll(getUuidList(columnRelationMap,sourceKey));
			}
			
			var references = inputRule.getReferences();
			if(Objects.nonNull(references) && !references.isEmpty()) {
				references.forEach(reference -> {
					var referenceType = reference.getType();
					var referenceSourceKey = reference.getSourceKey();
					if(sourcePredicate.test(referenceType) && columnRelationMap.containsKey(referenceSourceKey)) {
						columnsToBeValidatedMap.computeIfAbsent(dataSetId, key -> new ArrayList<String>()).addAll(getUuidList(columnRelationMap,referenceSourceKey));
					}
				});
			}			
		});
		
		return columnsToBeValidatedMap;
	}
	
	private List<String> getUuidList(Map<String, List<ColumnRelation>> columnRelationMap,String sourceKey){
		return	columnRelationMap.get(sourceKey).stream().map(cr -> cr.getUuid()).collect(Collectors.toList());
	}
	
	public Map<Integer, List<InputRule>> getOrderedRules(List<InputRule> inputRules) {
		return inputRules.stream()
				  .sorted(Comparator.comparingInt(InputRule::getSequence))
				  .collect(Collectors.groupingBy(InputRule::getSequence, LinkedHashMap::new, Collectors.toList()));
	}
	
	public void validateOrder(List<InputRule> tempOrderedInputRules) {
		tempOrderedInputRules.stream().forEach(inputRule -> {
			var isSources = Objects.nonNull(inputRule.getReferences()) ? inputRule.getReferences().parallelStream().allMatch(reference -> Objects.isNull(reference.getType()) || "source".equals(reference.getType())) : true;
			if(!(Objects.isNull(inputRule.getType()) || "source".equals(inputRule.getType()) && isSources)) {
				log.error("SharedFilesProcessorService -> process() invalid source type for input rule, uniqueId = {}",requestContext.getUniqueId());
				throw new ConfigurationException("invalid source type in input rules for sequence 0");
			}
		});
	}
	
	public Map<String, DataSet<Row>> process(List<InputRule> inputRules, Map<String, List<DataMap>> dataMap, List<DataSetRules> dataSetRules) {
		var dataSetMap = new HashMap<String, DataSet<Row>>();
		
		var orderedInputRules = getOrderedRules(inputRules);
		
		orderedInputRules.forEach((order, tempOrderedInputRules) -> {
			if(order == 0) {
				validateOrder(tempOrderedInputRules);
			}	
			var orderedDataSets = this.constructDataSets(tempOrderedInputRules, dataMap);		
			applyExclusions(orderedDataSets);
			var orderedDataSetMap = this.process(orderedDataSets, dataSetRules);	
			dataSetMap.putAll(orderedDataSetMap);
				
			if(orderedInputRules.size() > 1 && order < (orderedInputRules.size()-1)) {
				var orderdDataMap = orderedDataSetMap.entrySet().stream()
						.collect(HashMap<String,List<DataMap>>::new, (orderDataMap,entryDataSet) -> {
							var dataSet = entryDataSet.getValue();
							var dataMapList =  dataSet.getData().parallelStream().map(row -> {
								var columns =row.getColumns().entrySet().parallelStream().collect(HashMap<UUID,String>::new, 
										(map,entrySet) -> map.put(entrySet.getKey(), entrySet.getValue().getTargetValue()), 
										HashMap<UUID,String>::putAll);	
								var dataMapObj = new DataMap();
								dataMapObj.setColumns(columns);							
								return dataMapObj;
							}).collect(Collectors.toList());
							orderDataMap.put(entryDataSet.getKey(), dataMapList);
						}, HashMap<String,List<DataMap>>::putAll);					
				dataMap.putAll(orderdDataMap);
			}
		});
		return dataSetMap;
	}
		
}
