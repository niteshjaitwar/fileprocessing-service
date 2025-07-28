package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.config.model.InputRule;
import com.adp.esi.digitech.file.processing.enums.RequestStatus;
import com.adp.esi.digitech.file.processing.enums.TargetLocation;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ProcessException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.exception.TransformationException;
import com.adp.esi.digitech.file.processing.exception.ValidationException;
import com.adp.esi.digitech.file.processing.file.service.IFileService;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.Document;
import com.adp.esi.digitech.file.processing.model.ErrorData;
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.adp.esi.digitech.file.processing.util.FileValidationUtils;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;


@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class SharedFilesProcessorService extends AbstractProcessorService<ProcessResponse> {
	
	@Autowired
	ExcelProcessorService excelProcessorService;	
	
	IFileService iFilesService;
	
	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	FileValidationUtils fileValidationUtils;
	
	public void constructDefaults() {
		super.constructDefaults(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
	}

	@Override
	public ProcessResponse process(RequestPayload request) throws IOException, ReaderException, ConfigurationException, ValidationException, TransformationException, GenerationException, ProcessException {		
		log.info("SharedFilesProcessorService -> process() Received JSON request for processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
		
		this.iFilesService = objectUtilsService.objectProviderSharedFilesService.getObject(requestContext, TargetLocation.SharedDrive);
		
		this.initRequet(request);
		
		this.constructDefaults();
		
		this.validate(request);
		
		String inputRulesJson = configurationData.getInputRules();		
		String outputRulesJson = configurationData.getOutputFileRules();
		String filesInfoJson = configurationData.getFilesInfo();
		String datasetRulesJson = configurationData.getDataRules();
		
		List<InputRule> inputRules = this.getInputRules(inputRulesJson);
		Map<String,FileMetaData> fileMetaDataMap = ValidationUtil.isHavingValue(filesInfoJson) ? this.getFileMetaDataMap(filesInfoJson) : this.getFileMetaDataMap(inputRules);
		List<DataSetRules> dataSetRules = this.getDataSetRules(datasetRulesJson);
		
		log.info("SharedFilesProcessorService -> process() uniqueId = {}, fileMetaDataMap = {}", request.getUniqueId(), fileMetaDataMap);
		/*
		if(ValidationUtil.isHavingValue(filesInfo)) {
			fileMetaDataMap = this.getFilesInfo(filesInfo);
		} else {
			fileMetaDataMap = this.getHeaderIndexes(inputRules);
		}
		*/
		//List<OutputRule> outputRules = this.getOutputRules(outputRulesJson);
		
		List<ErrorData> errors = new ArrayList<>();
		
		var documents = request.getDocuments().parallelStream().map(document -> {
			try {
				MultipartFile file = iFilesService.getFile(document.getLocation(), appCode);
				document.setFile(file);
				return document;
			} catch (IOException e) {
				log.error("SharedFilesProcessorService -> process() Failed to get file from given location, uniqueId = {}, sourceKey = {}, location = {}, errorMessage = {}", request.getUniqueId(), document.getSourceKey(), document.getLocation(), e.getMessage());
				var error = new ErrorData(document.getSourceKey(), e.getMessage());
				errors.add(error);
				return null;
			}
		}).filter(Objects::nonNull).collect(Collectors.toList());
		
		if(documents.size() != request.getDocuments().size()) {
			var validationException = new ValidationException("Request Validation - Failed while reading file from the Network location");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
		
		//Perform Metadata Validation
		var isMetaValidationsFound = documents.parallelStream().map(document -> {
			MultipartFile sharedFile = document.getFile();	
			var sourceKey = document.getSourceKey();
			var fileMetaData = fileMetaDataMap.get(sourceKey);
			
			if(fileMetaData == null) {
				var optFileMetaData = fileMetaDataMap.keySet().parallelStream().filter(key -> key.startsWith(sourceKey)).findFirst();				
				fileMetaData = optFileMetaData.isPresent() ? fileMetaDataMap.get(optFileMetaData.get()): null;
			}
			
			var fileExtension = fileMetaData != null ? fileMetaData.getType() : fileUtils.getFileExtension(sharedFile.getOriginalFilename());
			return fileValidationUtils.validate(document.getFile(), fileExtension.toLowerCase());
		}).reduce(false, Boolean::logicalOr);		
		
		if(isMetaValidationsFound) {
			var metadataValidationException = new MetadataValidationException("Failed to process basic validation like file type and size");
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		/*
		//Perform Header Validation
		var headerErrors = documents.parallelStream().map(document -> {			
			MultipartFile sharedFile = document.getFile();
			var fileNameKey = document.getSourceKey();
			var fileExtension = fileUtils.getFileExtension(sharedFile.getOriginalFilename());
				
			switch (fileExtension.toLowerCase()) {
				case "xlsx":
					try(XSSFWorkbook workBook = new XSSFWorkbook(sharedFile.getInputStream())) {						
						int noOfSheets = workBook.getNumberOfSheets();					
						var excelErrors = IntStream.range(0, noOfSheets).parallel().mapToObj(index -> {
							var sheet = workBook.getSheetAt(index);
							var key = (noOfSheets == 1) ? fileNameKey : fileNameKey.concat("{{").concat(sheet.getSheetName()).concat("}}");	
							log.info("SharedFilesProcessorService -> process() uniqueId = {}, key = {}, headerIndexMap = {}", request.getUniqueId(), key, fileMetaDataMap);
							try {
								if (!columnRelationMap.containsKey(key))
									throw new MetadataValidationException("Request Validation - File Name in the input request is not matching with sourceKey present in the configuration");
			
								var dbHeaders = columnRelationMap.get(key).parallelStream().map(item -> item.getColumnName()).collect(Collectors.toList());
								
								validate(sheet, fileMetaDataMap.get(key).getHeaderIndex(), dbHeaders);
							} catch (MetadataValidationException e) {
								return new ErrorData(key, e.getMessage());
							}
							return null;
						}).filter(Objects::nonNull).collect(Collectors.toList());
								
						if(excelErrors != null && !excelErrors.isEmpty())
							log.error("SharedFilesProcessorService -> process() Failed validating excel headers, uniqueId = {}, errors = {}",request.getUniqueId(), excelErrors);
						return excelErrors;
					} catch (IOException e) {
						log.error("SharedFilesProcessorService -> process() Failed process excel file, uniqueId = {}, fileName = {}, error = {}",request.getUniqueId(), sharedFile.getOriginalFilename(), e.getMessage());
						return List.of(new ErrorData(sharedFile.getOriginalFilename(), e.getMessage()));
					}
				case "csv":
					
					var size = columnRelationMap.get(fileNameKey).size();
					
					try(BufferedReader br = new BufferedReader(new InputStreamReader(sharedFile.getInputStream()))) {
						var fileMetadataInfo = fileMetaDataMap.get(fileNameKey);
                        String header = br.readLine();                                               
                        if(header == null) {
                            return List.of(new ErrorData(fileNameKey, "There is a mismatch between column configued and data in " + fileNameKey));
                        }                        
                        String[] headers = header.split(fileMetadataInfo.getDelimeter(), -1);               
                        if(headers.length != size) {
                            return List.of(new ErrorData(fileNameKey, "There is a mismatch between column configued and data in " + fileNameKey));
                        }
                    } catch (IOException e) {
                    	return List.of(new ErrorData(sharedFile.getOriginalFilename(), e.getMessage()));
					}
					
					return null;
						
				case "json":
					return null;
						
				default:
					return null;
					
			}
		}).filter(Objects::nonNull).flatMap(List::stream).collect(Collectors.toList());
		
		if(headerErrors != null && !headerErrors.isEmpty()) {
			log.error("SharedFilesProcessorService -> process() Failed validating headers, uniqueId = {}, errors = {}",request.getUniqueId(), headerErrors);
			var metadataValidationException = new MetadataValidationException("Headers Validation Failed");
			metadataValidationException.setErrors(headerErrors);
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		
		log.info("SharedFilesProcessorService -> process() Completed validating headers, uniqueId = {}",request.getUniqueId());
		*/
		Map<String, List<DataMap>> dataMap = new HashMap<>();
		errors.clear();
		for (Document document : documents) {
			try {
				MultipartFile sharedFile = document.getFile();					
				var fileNameKey = document.getSourceKey();
				var fileExtension = fileUtils.getFileExtension(sharedFile.getOriginalFilename());
				switch (fileExtension.toLowerCase()) {
					case "xlsx":
						try(XSSFWorkbook workBook = new XSSFWorkbook(sharedFile.getInputStream())) {
							var excelFileMetaDataMap = getFileMetaDataMap(fileMetaDataMap, fileNameKey);						
							var xlsxData = objectUtilsService.customReaderDynamicAutowireService.readWorkbookData(workBook, fileNameKey, columnRelationMap, excelFileMetaDataMap, this.requestContext);							
							dataMap.putAll(xlsxData);
						} catch (IOException e) {
							log.error("SharedFilesProcessorService -> process() Failed process excel file, uniqueId = {}, fileName = {}, error = {}",request.getUniqueId(), sharedFile.getOriginalFilename(), e.getMessage());
						}
						break;
					case "csv":
						var csvFileMetadataInfo = fileMetaDataMap.get(fileNameKey);
						var csvData = objectUtilsService.customReaderDynamicAutowireService.readCSVData(sharedFile, csvFileMetadataInfo, columnRelationMap.get(fileNameKey), this.requestContext);					
						dataMap.putAll(csvData);
						break;
					case "txt": 						
					case "dat":
						var txtFileMetadataInfo = fileMetaDataMap.get(fileNameKey);
						var txtColumnRelations = columnRelationMap.entrySet().stream().filter(entry -> entry.getKey().startsWith(fileNameKey)).flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList());
						var txtData = objectUtilsService.customReaderDynamicAutowireService.readTXTData(sharedFile, txtFileMetadataInfo, txtColumnRelations, this.requestContext);					
						dataMap.putAll(txtData);
						break;
					case "xml":
                        var fileMetadataInfoXml = fileMetaDataMap.get(fileNameKey);
                        var xmlColumnRelations = columnRelationMap.values().stream().flatMap(List::stream).filter(cr -> cr.getSourceKey().startsWith(fileNameKey)).collect(Collectors.toList());
                        var xmlData = objectUtilsService.customReaderDynamicAutowireService.readXMLData(sharedFile, fileMetadataInfoXml, xmlColumnRelations, this.requestContext);                    
                        dataMap.putAll(xmlData);
                        break;
					case "json":
						break;
							
					default:
						break;
					
				}
			} catch (MetadataValidationException e) {
				errors.addAll(e.getErrors());
			}
		}
		if(!errors.isEmpty()) {
			var metadataValidationException = new MetadataValidationException("Headers Validation Failed");
			metadataValidationException.setRequestContext(requestContext);
			metadataValidationException.setErrors(errors);
			throw metadataValidationException;
		}
		/*
		Map<String, List<DataMap>> dataMap1 =  documents.parallelStream().map(document -> {
			
			MultipartFile sharedFile = document.getFile();				
			//var fileName = fileUtils.getFileName(sharedFile.getOriginalFilename());	
			var fileNameKey = document.getSourceKey();
			var fileExtension = fileUtils.getFileExtension(sharedFile.getOriginalFilename());				
			switch (fileExtension.toLowerCase()) {
				case "xlsx":
					try(XSSFWorkbook workBook = new XSSFWorkbook(sharedFile.getInputStream())) {
						var excelFileMetaDataMap = getFileMetaDataMap(fileMetaDataMap, fileNameKey);						
						return  objectUtilsService.customReaderDynamicAutowireService.readWorkbookData(workBook, fileNameKey, columnRelationMap, excelFileMetaDataMap, this.requestContext);							
					} catch (IOException e) {
						log.error("SharedFilesProcessorService -> process() Failed process excel file, uniqueId = {}, fileName = {}, error = {}",request.getUniqueId(), sharedFile.getOriginalFilename(), e.getMessage());
					}
				case "csv":
					var fileMetadataInfo = fileMetaDataMap.get(fileNameKey);
					return objectUtilsService.customReaderDynamicAutowireService.readCSVData(sharedFile, fileMetadataInfo, columnRelationMap.get(fileNameKey), this.requestContext);					
					
				case "txt": 
				case "dat":
					return null;
				case "json":
					return null;
						
				default:
					return null;
					
			}
		}).filter(Objects::nonNull).flatMap(m -> m.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		*/
		log.info("SharedFilesProcessorService -> process() Completed reading files data, uniqueId = {}",request.getUniqueId());
				
		
		
		if(ValidationUtil.isHavingValue(request.getFormData())) {
			var formJson = new JSONObject(request.getFormData());
			var formKey = "Form Data";
			var tempFormMap = objectUtilsService.customReaderDynamicAutowireService.readFormData(formJson, fileMetaDataMap.get(formKey), columnRelationMap.get(formKey), requestContext);
			dataMap.putAll(tempFormMap);			
		}
		
		columnsToValidateMap = getSourceColumnsToValidate(inputRules);
		var dataSetMap = process(inputRules, dataMap, dataSetRules);
		
	/*	var dataSets = this.constructDataSets(inputRules, dataMap);		
		applyExclusions(dataSets);
		
		log.info("SharedFilesProcessorService -> process() Completed constructing dataSets, uniqueId = {}",request.getUniqueId());
		
		//Apply Data Exclusions
		
		var dataSetMap = this.process(dataSets, dataSetRules);		
	*/	
		log.info("JSONProcessorService -> process() Completed transforming dataSets, uniqueId = {}",request.getUniqueId());
		
		var sharedFiles = this.generate(dataSetMap, outputRulesJson);
		
		log.info("SharedFilesProcessorService -> process() Completed constructing files, uniqueId = {}",request.getUniqueId());
		
		this.send(sharedFiles, fileProcessType);
		
		this.sendEmail(sharedFiles);
		
		this.updateRequest(request, RequestStatus.Completed);
		
		documents.parallelStream().forEach(document -> {
			this.clean(document.getLocation(),request.getUniqueId());
		});
		
		log.info("SharedFilesProcessorService -> process() Completed processing, uniqueId = {}", request.getUniqueId());	
		return returnProcessResponse(sharedFiles);
	}
	
	@Override
	public void validate(RequestPayload request) throws ValidationException {
		log.info("SharedFilesProcessorService - process()  Started checking files from the given location. uniqueId = {}, appCode = {}", requestContext.getUniqueId(), appCode);
		//boolean isFound = request.getDocuments().parallelStream().allMatch(document -> sharedFilesService.isFileExists(document.getLocation(), appCode));
		if(request.getDocuments() == null || request.getDocuments().isEmpty()) {
			List<ErrorData> errors = new ArrayList<>();
			errors.add(new ErrorData("documents", "Documents can't be null or empty"));
			var validationException = new ValidationException("Request Validation - Documents can't be null or empty");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
		var errors = request.getDocuments().parallelStream().map(document -> {
			if(!iFilesService.isFileExists(document.getLocation(), appCode))
				return new ErrorData(document.getSourceKey(), document.getLocation());
			return null;
		}).filter(Objects::nonNull).collect(Collectors.toList());
		
		if (errors != null && !errors.isEmpty()) {
			var validationException = new ValidationException("Request Validation - Files not present in Shared Location");
			validationException.setErrors(errors);
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
	}
	
	@Override
	public Map<String, List<DataMap>> read(RequestPayload data) throws ReaderException {
		return null;
	}

	

	public void setiFilesService(IFileService iFilesService) {
		this.iFilesService = iFilesService;
	}
	
	
}