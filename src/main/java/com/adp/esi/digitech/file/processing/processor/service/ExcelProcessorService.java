package com.adp.esi.digitech.file.processing.processor.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.adp.esi.digitech.file.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
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
import com.adp.esi.digitech.file.processing.model.ProcessResponse;
import com.adp.esi.digitech.file.processing.model.RequestPayload;
import com.adp.esi.digitech.file.processing.util.FileUtils;
import com.adp.esi.digitech.file.processing.util.FileValidationUtils;
import com.adp.esi.digitech.file.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service("excelProcessorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ExcelProcessorService extends AbstractProcessorService<ProcessResponse> {
		
	@Autowired
	FileUtils fileUtils;
	
	@Autowired
	FileValidationUtils fileValidationUtils;
	
	public void constructDefaults() {
		super.constructDefaults(requestContext.getBu(), requestContext.getPlatform(), requestContext.getDataCategory());
	}

	@Override
	public ProcessResponse process(RequestPayload request) throws IOException, ReaderException, ConfigurationException, ValidationException, TransformationException, GenerationException, ProcessException {
		log.info("ExcelProcessorService -> process() Received Excel request for processing, uniqueId = {}, request = {}", request.getUniqueId(), request);
		
		this.initRequet(request);
		
		this.constructDefaults();
		// Step 1: Validating request
		
		this.validate(request);
		
		
		String inputRulesJson = configurationData.getInputRules();		
		String outputRulesJson = configurationData.getOutputFileRules();
		String filesInfo = configurationData.getFilesInfo();
		String datasetRulesJson = configurationData.getDataRules();
		
		List<InputRule> inputRules = this.getInputRules(inputRulesJson);
		Map<String,FileMetaData> fileMetaDataMap = ValidationUtil.isHavingValue(filesInfo) ? this.getFileMetaDataMap(filesInfo) : this.getFileMetaDataMap(inputRules);
		List<DataSetRules> dataSetRules = this.getDataSetRules(datasetRulesJson);
		//List<OutputRule> outputRules = this.getOutputRules(outputRulesJson);
		
		
		MultipartFile file = request.getFile();
		//String fileName = file.getName();
		XSSFWorkbook workBook = new XSSFWorkbook(file.getInputStream());
		
		
		//Step 3: Validating Headers
		//Map<String,List<ColumnRelation>> sourceKeyColumnRelationsMap = objectUtilsService.columnRelationService.findBy(request.getBu(), request.getPlatform(),request.getDataCategory(), fileName);
		
		
		//int noOfSheets = workBook.getNumberOfSheets();
		var fileName = fileUtils.getFileName(request.getFile().getOriginalFilename());
		/*
		if(noOfSheets == 1) {
			var sheet = workBook.getSheetAt(0);
			var key = fileName;
			log.info("ExcelProcessorService - process()  Started processing key = {}", key);
			if (!columnRelationMap.containsKey(key)) {
				workBook.close();
				throw new MetadataValidationException("Request Validation - File Name in the input request is not matching with sourceKey present in the configuration");
			}
			var dbHeaders = columnRelationMap.get(key).parallelStream().map(item -> item.getColumnName()).collect(Collectors.toList());			
			
			validate(sheet, headerIndexMap.get(key), dbHeaders);				
		}
		
		var errors = IntStream.range(0, noOfSheets).parallel().mapToObj(index -> {
			var sheet = workBook.getSheetAt(index);
			var key = (noOfSheets == 1) ? fileName : fileName.concat("{{").concat(sheet.getSheetName()).concat("}}");
			//var key = fileName.concat("{{").concat(sheet.getSheetName()).concat("}}");
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
			
		if(errors != null && !errors.isEmpty()) {
			log.error("ExcelProcessorService -> process() Failed validating headers, uniqueId = {}, errors = {}",request.getUniqueId(), errors);
			workBook.close();
			var metadataValidationException = new MetadataValidationException("Headers Validation Failed");
			metadataValidationException.setErrors(errors);
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		
		
		log.info("ExcelProcessorService -> process() Completed validating headers, uniqueId = {}",request.getUniqueId());
		*/
		//Step 4: Parse Excel Sheet Data to List<Row>
		var excelFileMetaDataMap = getFileMetaDataMap(fileMetaDataMap, fileName);			
		Map<String, List<DataMap>> dataMap = objectUtilsService.customReaderDynamicAutowireService.readWorkbookData(workBook, fileName, columnRelationMap, excelFileMetaDataMap, this.requestContext);
		workBook.close();
		log.info("ExcelProcessorService -> process() Completed reading Excel data, uniqueId = {}",request.getUniqueId());
		
		columnsToValidateMap = getSourceColumnsToValidate(inputRules);
		var dataSetMap = process(inputRules, dataMap, dataSetRules);
		
		/*
		var dataSets = this.constructDataSets(inputRules, dataMap);
		applyExclusions(dataSets);
		
		log.info("ExcelProcessorService -> process() Completed constructing dataSets, uniqueId = {}",request.getUniqueId());
		
		//Apply Data Exclusions
		
		var dataSetMap = this.process(dataSets, dataSetRules);	
		*/
		
		log.info("ExcelProcessorService -> process() Completed transforming dataSets, uniqueId = {}",request.getUniqueId());
		
		var sharedFiles = this.generate(dataSetMap, outputRulesJson);
		
		log.info("ExcelProcessorService -> process() Completed constructing files, uniqueId = {}",request.getUniqueId());
		
		this.send(sharedFiles, fileProcessType);
		
		this.sendEmail(sharedFiles);
		
		this.updateRequest(request, RequestStatus.Completed);
		
		log.info("ExcelProcessorService -> process() Completed processing, uniqueId = {}", request.getUniqueId());	
		
		return returnProcessResponse(sharedFiles);
	}
	
	@Override
	public void validate(RequestPayload request) throws ValidationException {	
		if(request.getFile() == null) {
			var validationException = new ValidationException("Request Validation - File is required");
			validationException.setRequestContext(requestContext);
			throw validationException;
		}
		
		// Step 2: Validating size and file type
		validate(request.getFile());
	}

	private void validate(MultipartFile file) throws MetadataValidationException {
		if(fileValidationUtils.validate(file, "xlsx")) {
			var metadataValidationException = new MetadataValidationException("Failed to process basic validation like file type and size");
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		//objectUtilsService.customValidatorDynamicAutowireService.validate(ExcelMetadataValidationService.class, file, this.requestContext);
	}

	
}
