package com.adp.esi.digitech.file.processing.reader.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.autowire.service.CustomReaderDynamicAutowireService;
import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.exception.ReaderException;
import com.adp.esi.digitech.file.processing.model.DataMap;
import com.adp.esi.digitech.file.processing.model.ErrorData;

import lombok.extern.slf4j.Slf4j;

@Service("excelReaderService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ExcelReaderService extends AbstractExcelReaderService<DataMap, XSSFWorkbook> {
	
	private String fileName;
	
	private Map<String, List<ColumnRelation>> columnRelationsMap;
	
	private Map<String, FileMetaData> fileMetaDataMap;
	
	@Autowired
	CustomReaderDynamicAutowireService customReaderDynamicAutowireService;
	
	@Autowired(required = true)
	public ExcelReaderService(String fileName, Map<String, List<ColumnRelation>> columnRelationsMap, Map<String, FileMetaData> fileMetaDataMap) {
		this.fileName = fileName;
		this.columnRelationsMap = columnRelationsMap;
		this.fileMetaDataMap = fileMetaDataMap;
	}

	@Override
	public Map<String, List<DataMap>> read(XSSFWorkbook workbook) throws ReaderException {
		try {			
			if (workbook == null || workbook.getNumberOfSheets() <= 0)
				throw new IllegalArgumentException(fileName + " Excel Must contain atleast one sheet");
			
			log.info("ExcelReaderService -> read() Started processing excel, uniqueId = {}, fileName = {}", requestContext.getUniqueId(), fileName);
			
			int noOfSheets = workbook.getNumberOfSheets();

			if(fileMetaDataMap.size() == 1 && noOfSheets == 1) {
				var sheet = workbook.getSheetAt(0);
				var sourceKey = fileName;
				var columnRelations = columnRelationsMap.get(sourceKey);
				var fileMetaData = fileMetaDataMap.get(sourceKey);
				if(Objects.isNull(fileMetaData.getSourceKey())) {
					fileMetaData.setSourceKey(sourceKey);
				}
				var sheetData = customReaderDynamicAutowireService.readSheetData(sheet, fileMetaData, columnRelations, this.requestContext);				
				workbook.close();
				log.info("ExcelReaderService -> read() Completed processing excel, uniqueId = {}, fileName = {}", requestContext.getUniqueId(), fileName);
				return sheetData;
			}
			

			//Checking for configured sheets in the excel, If any configured sheet missing in excel we are throwing error
			var invalidFileMetadataList = fileMetaDataMap.entrySet().stream().filter(entry -> {
				var key = entry.getKey();
				var sheetName = key.contains("{{") && key.endsWith("}}") ? key.substring(key.indexOf("{{") + 2, key.indexOf("}}")) : key;
				return workbook.getSheet(sheetName) == null;
            }).map(entry -> entry.getValue()).collect(Collectors.toList());
			
			
			if(Objects.nonNull(invalidFileMetadataList) && !invalidFileMetadataList.isEmpty()) {
				createMetadataValidationExceptionSheet(invalidFileMetadataList);
			}
			
			Map<String, List<DataMap>> data = new HashedMap<>();
			var errors = new ArrayList<ErrorData>();
			fileMetaDataMap.forEach((key, fileMetaData) -> {
				try {
					var sheetName = key.substring(key.indexOf("{{") + 2, key.indexOf("}}"));
					var sheet = workbook.getSheet(sheetName);
					var columnRelations = columnRelationsMap.get(key);
					var sheetData = customReaderDynamicAutowireService.readSheetData(sheet, fileMetaData, columnRelations, this.requestContext);
					data.putAll(sheetData);
				} catch(MetadataValidationException e) {
					errors.addAll(e.getErrors());
				}
			});
			
			workbook.close();
			
			if(!errors.isEmpty()) {
				throw createMetadataValidationException(errors);
			}
			log.info("ExcelReaderService -> read() Completed processing excel, uniqueId = {}, fileName = {}", requestContext.getUniqueId(), fileName);
			return data;
		} catch (IOException e) {
			log.error("ExcelReaderService -> read() Failed to processing excel, uniqueId = {}, fileName = {}, message = {}", requestContext.getUniqueId(), fileName, e.getMessage());
			var readerException = new ReaderException("Excel Parsing failed, reason = " + e.getMessage(), e.getCause());
			readerException.setRequestContext(requestContext);
			throw readerException;
		}

	}
}
